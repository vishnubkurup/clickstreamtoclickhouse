import logging.config
import shutil
from os import path

import click

from .config import configuration
from .jobs import send_to_warehouse
from .util import aws_wrapper

log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger(__name__)


@click.group()
def app():
    """Send SegmentSpec event files to Warehouses"""


@app.command()
@click.option("--config-file", "-cf", type=click.Path(exists=True))
@click.option("--s3-dir", "-s3d",
              help="S3 Directory. We will look for *.gz files in this directory. "
                   "Ensure that you have configured aws "
                   "credentials using aws cli. "
                   "Make sure that this directory contains files less than 100.")
@click.option("--source-dir", "-sd", type=click.Path(exists=True))
@click.option("--namespace", "-ns", required=True, help="Will be used to create database/namespace in warehouse", )
def send(config_file: str, s3_dir: str, source_dir: str, namespace: str):
    """Send Segment Files to different warehouses """
    logger.info(f"config_file={config_file}")
    app_conf = configuration.from_yaml(config_file)

    try:
        if s3_dir:
            source_dir = aws_wrapper.download_gz_files(s3_dir)

        job = send_to_warehouse.SendToWarehouseJob(app_conf, source_dir, namespace)
        job.execute()
    finally:
        if s3_dir:
            logger.info(f"Removing directory {source_dir}")
            shutil.rmtree(source_dir)