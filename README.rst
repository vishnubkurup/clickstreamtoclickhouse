SegHouse
=========

Send Segment event from S3 files to warehouse.

Features
========
- Stores Segment events from S3 files to Data Warehouse
- Supported warehouses
    - ClickHouse
- Fixes data type issues

Installation
============
This tool depends on `awscli`.

- :code:`pip3 install awscli`
- :code:`aws configure`

Install SegHouse.

- `pip install seghouse`

Python version >= 3.7 is required. If installed python version is less than 3.7 then first install python3.7.

- :code:`sudo apt install python3.7`
- :code:`sudo apt-get install build-essential libssl-dev libffi-dev python3.7-dev`
- Then install seghouse using `python3.7 -m pip install seghouse` command.
- Remember to run all SegHouse commands using python3.7. Example
    :code:`python3.7 -m seghouse send ...`

Commands Overview
=================
- Send s3 segment files to warehouse.
    - Command : :code:`seghouse send --config-file ~/example-seghouse-config.yml --s3-dir "s3://company/clickstream/example_app/android" --namespace example_app_android`
    - The command expects to find `json.gz` files in the S3 path. All these files will be parsed according to Segment Spec and events will be stored in destination warehouses.
    - The configuration file looks like this.

.. code-block:: yaml

    # Configure the warehouses credentials here.
    # The user should have create database, create table and add column permissions.
    warehouses:
      - type: clickhouse
        host: clickhouse_host_ip
        port: 9000
        user: clickhouse_user
        password: clickhouse_password

    # Specify fields that should be skipped
    skip_fields:
      - 'field_to_be_skipped_1'
      - 'field_to_be_skipped_2'

    # Specify additional timestamp fields
    # A new field will be created by converting timestamp to given timezone
    extra_timestamps:
      timestamp_ist: Asia/Kolkata
