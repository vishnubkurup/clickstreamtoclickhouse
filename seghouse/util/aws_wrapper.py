import logging
import subprocess
import uuid

logger = logging.getLogger(__name__)

TMP_DIR_PREFIX = "seghouse"


def s3_copy(s3_path, local_dir_path):
    #command = ["aws", "s3", "cp", s3_path, local_dir_path, "--recursive", "--exclude", "*", "--include", "*.gz"]
    command = ["aws", "s3", "cp", s3_path, local_dir_path, "--recursive"]
    logger.info(f"command = {command}")
    process = subprocess.run(command)
    process.check_returncode()


def download_gz_files(s3_dir):
    local_dir_name = str(uuid.uuid4()).replace("-", "")
    local_dir_path = f"/tmp/{TMP_DIR_PREFIX}-{local_dir_name}"
    subprocess.run(["mkdir", "-p", local_dir_path])

    logger.info(f"Copying files to {local_dir_path}")
    s3_copy(s3_dir, local_dir_path)

    return local_dir_path
