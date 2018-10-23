import json
import os
import logging
from random import choice
import boto3
from botocore.exceptions import ClientError

from postgresql import Postgresql
from mysql import Mysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def log_error_with_event(err_msg, event, info=False):
    """Log the error message and event to AWSCloudWatch."""
    if info:
        logger.info("%s: %s", err_msg, event)
    else:
        logger.error("%s: %s", err_msg, event)

    return err_msg


def get_secret(secret_name):
    """Return the requested secret."""
    aws_conn = boto3.client('secretsmanager')

    try:
        get_secret_value_response = aws_conn.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as err:
        err_msg = err.response['Error']['Code']
        if err.response['Error']['Code'] == 'ResourceNotFoundException':
            err_msg = "The requested secret " + secret_name + " was not found"
        elif err.response['Error']['Code'] == 'InvalidRequestException':
            err_msg = "The request was invalid due to"
        elif err.response['Error']['Code'] == 'InvalidParameterException':
            err_msg = "The request had invalid params"
        elif err.response['Error']['Code'] == 'AccessDeniedException':
            err_msg = "The request was invalid due to Access Denied"

        log_error_with_event("Failed to get secret", err_msg)

        return None

    else:
        # Decrypted secret using the associated KMS CMK
        # Depending on whether the secret was a string or binary
        secret = []
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']

            # Tries to convert the secret to a Dict type
            try:
                secret = json.loads(secret)
            except (TypeError, json.decoder.JSONDecodeError) as err:
                err_msg = "Could not convert secret to Dict"
                return log_error_with_event(err_msg, err)

        else:
            secret = get_secret_value_response['SecretBinary']

        return secret


def create_new_user(db_instance, user, passw):
    """Create a new user on the DB"""
    return db_instance.create_user(user, passw)


def get_new_password(db_instance, user, passw):
    return db_instance.get_new_password(user, passw)


def lambda_handler(event, _):
    try:
        db_name = event["db_name"]
        db_type = event["db_type"]
        username = event["username"]

    except KeyError:
        return "Missing parameters. Specify DB name, region,\
type (mysql or postgresql) and username"

    else:
        secret = get_secret(os.getenv('DB_PREFIX', '') + db_name)
        db_instance = None
        if db_type == 'mysql':
            db_instance = Mysql(**secret)
        elif db_type == 'postgresql':
            db_instance = Postgresql(**secret)
        
        db_info = {
            'db_url': secret.get("host"),
            'db_port': secret.get("port"),
            'db_name': secret.get("dbname")
        }

        passw = ''.join([
                choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')
            for i in range(32)])

        db_info["username"] = username
        db_info["passw"] = passw
           
        if not get_new_password(db_instance, username, passw):
            create_new_user(db_instance, username, passw)
            
        return db_info
    
    return None