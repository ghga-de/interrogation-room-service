[![tests](https://github.com/ghga-de/interrogation-room-service/actions/workflows/tests.yaml/badge.svg)](https://github.com/ghga-de/interrogation-room-service/actions/workflows/tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/interrogation-room-service/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/interrogation-room-service?branch=main)

# Interrogation Room Service

Interrogation Room Service

## Description

The Interrogation Room Service (IRS) interfaces with the Encryption Key Store Service to process Crypt4GH encrypted files uploaded to the inbox of a local GHGA node.
The IRS splits off the file envelope, computes part checksums over the encrypted file content, validates the checksum over the unencrypted file content (in memory) and initiates transfer of the encrypted file content to its permanent storage.


## Installation

We recommend using the provided Docker container.

A pre-build version is available at [docker hub](https://hub.docker.com/repository/docker/ghga/interrogation-room-service):
```bash
docker pull ghga/interrogation-room-service:2.1.0
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
docker build -t ghga/interrogation-room-service:2.1.0 .
```

For production-ready deployment, we recommend using Kubernetes, however,
for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is preconfigured:
docker run -p 8080:8080 ghga/interrogation-room-service:2.1.0 --help
```

If you prefer not to use containers, you may install the service from source:
```bash
# Execute in the repo's root dir:
pip install .

# To run the service:
irs --help
```

## Configuration

### Parameters

The service requires the following configuration parameters:
- **`object_stale_after_minutes`** *(integer)*: Amount of time in minutes after which an object in the staging bucket is considered stale. If an object continues existing after this point in time, this is an indication, that something might have gone wrong downstream.

- **`log_level`** *(string)*: The minimum log level to capture. Must be one of: `["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "TRACE"]`. Default: `"INFO"`.

- **`service_name`** *(string)*: Default: `"irs"`.

- **`service_instance_id`** *(string)*: A string that uniquely identifies this instance across all instances of this service. A globally unique Kafka client ID will be created by concatenating the service_name and the service_instance_id.


  Examples:

  ```json
  "germany-bw-instance-001"
  ```


- **`log_format`**: If set, will replace JSON formatting with the specified string format. If not set, has no effect. In addition to the standard attributes, the following can also be specified: timestamp, service, instance, level, correlation_id, and details. Default: `null`.

  - **Any of**

    - *string*

    - *null*


  Examples:

  ```json
  "%(timestamp)s - %(service)s - %(level)s - %(message)s"
  ```


  ```json
  "%(asctime)s - Severity: %(levelno)s - %(msg)s"
  ```


- **`interrogation_topic`** *(string)*: Name of the topic used for events informing about the outcome of file validations.


  Examples:

  ```json
  "file_interrogation"
  ```


- **`interrogation_success_type`** *(string)*: The type used for events informing about the success of a file validation.


  Examples:

  ```json
  "file_validation_success"
  ```


- **`interrogation_failure_type`** *(string)*: The type used for events informing about the failure of a file validation.


  Examples:

  ```json
  "file_validation_failure"
  ```


- **`file_registered_event_topic`** *(string)*: Name of the topic used for events indicating that a new file has been internally registered.


  Examples:

  ```json
  "internal_file_registry"
  ```


- **`file_registered_event_type`** *(string)*: The type used for events indicating that a new file has been internally registered.


  Examples:

  ```json
  "file_registered"
  ```


- **`upload_received_event_topic`** *(string)*: Name of the topic to publish events that inform about new file uploads.


  Examples:

  ```json
  "file_uploads"
  ```


- **`upload_received_event_type`** *(string)*: The type to use for events that inform about new file uploads.


  Examples:

  ```json
  "file_upload_received"
  ```


- **`object_storages`** *(object)*: Can contain additional properties.

  - **Additional properties**: Refer to *[#/$defs/S3ObjectStorageNodeConfig](#%24defs/S3ObjectStorageNodeConfig)*.

- **`db_connection_str`** *(string, format: password)*: MongoDB connection string. Might include credentials. For more information see: https://naiveskill.com/mongodb-connection-string/.


  Examples:

  ```json
  "mongodb://localhost:27017"
  ```


- **`db_name`** *(string)*: Name of the database located on the MongoDB server.


  Examples:

  ```json
  "my-database"
  ```


- **`kafka_servers`** *(array)*: A list of connection strings to connect to Kafka bootstrap servers.

  - **Items** *(string)*


  Examples:

  ```json
  [
      "localhost:9092"
  ]
  ```


- **`kafka_security_protocol`** *(string)*: Protocol used to communicate with brokers. Valid values are: PLAINTEXT, SSL. Must be one of: `["PLAINTEXT", "SSL"]`. Default: `"PLAINTEXT"`.

- **`kafka_ssl_cafile`** *(string)*: Certificate Authority file path containing certificates used to sign broker certificates. If a CA is not specified, the default system CA will be used if found by OpenSSL. Default: `""`.

- **`kafka_ssl_certfile`** *(string)*: Optional filename of client certificate, as well as any CA certificates needed to establish the certificate's authenticity. Default: `""`.

- **`kafka_ssl_keyfile`** *(string)*: Optional filename containing the client private key. Default: `""`.

- **`kafka_ssl_password`** *(string)*: Optional password to be used for the client private key. Default: `""`.

- **`generate_correlation_id`** *(boolean)*: A flag, which, if False, will result in an error when trying to publish an event without a valid correlation ID set for the context. If True, the a newly correlation ID will be generated and used in the event header. Default: `true`.


  Examples:

  ```json
  true
  ```


  ```json
  false
  ```


- **`ekss_base_url`** *(string)*: URL pointing to the Encryption Key Store service.


  Examples:

  ```json
  "http://ekss:8080"
  ```


## Definitions


- <a id="%24defs/S3Config"></a>**`S3Config`** *(object)*: S3-specific config params.
Inherit your config class from this class if you need
to talk to an S3 service in the backend.<br>  Args:
    s3_endpoint_url (str): The URL to the S3 endpoint.
    s3_access_key_id (str):
        Part of credentials for login into the S3 service. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    s3_secret_access_key (str):
        Part of credentials for login into the S3 service. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    s3_session_token (Optional[str]):
        Optional part of credentials for login into the S3 service. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    aws_config_ini (Optional[Path]):
        Path to a config file for specifying more advanced S3 parameters.
        This should follow the format described here:
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file
        Defaults to None. Cannot contain additional properties.

  - **`s3_endpoint_url`** *(string, required)*: URL to the S3 API.


    Examples:

    ```json
    "http://localhost:4566"
    ```


  - **`s3_access_key_id`** *(string, required)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.


    Examples:

    ```json
    "my-access-key-id"
    ```


  - **`s3_secret_access_key`** *(string, format: password, required)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.


    Examples:

    ```json
    "my-secret-access-key"
    ```


  - **`s3_session_token`**: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html. Default: `null`.

    - **Any of**

      - *string, format: password*

      - *null*


    Examples:

    ```json
    "my-session-token"
    ```


  - **`aws_config_ini`**: Path to a config file for specifying more advanced S3 parameters. This should follow the format described here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file. Default: `null`.

    - **Any of**

      - *string, format: path*

      - *null*


    Examples:

    ```json
    "~/.aws/config"
    ```


- <a id="%24defs/S3ObjectStorageNodeConfig"></a>**`S3ObjectStorageNodeConfig`** *(object)*: Configuration for one specific object storage node and one bucket in it.<br>  The bucket is the main bucket that the service is responsible for. Cannot contain additional properties.

  - **`bucket`** *(string, required)*

  - **`credentials`**: Refer to *[#/$defs/S3Config](#%24defs/S3Config)*.


### Usage:

A template YAML for configurating the service can be found at
[`./example-config.yaml`](./example-config.yaml).
Please adapt it, rename it to `.irs.yaml`, and place it into one of the following locations:
- in the current working directory were you are execute the service (on unix: `./.irs.yaml`)
- in your home directory (on unix: `~/.irs.yaml`)

The config yaml will be automatically parsed by the service.

**Important: If you are using containers, the locations refer to paths within the container.**

All parameters mentioned in the [`./example-config.yaml`](./example-config.yaml)
could also be set using environment variables or file secrets.

For naming the environment variables, just prefix the parameter name with `irs_`,
e.g. for the `host` set an environment variable named `irs_host`
(you may use both upper or lower cases, however, it is standard to define all env
variables in upper cases).

To using file secrets please refer to the
[corresponding section](https://pydantic-docs.helpmanual.io/usage/settings/#secret-support)
of the pydantic documentation.



## Architecture and Design:
This is a Python-based service following the Triple Hexagonal Architecture pattern.
It uses protocol/provider pairs and dependency injection mechanisms provided by the
[hexkit](https://github.com/ghga-de/hexkit) library.


## Development

For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of VS Code
in combination with Docker Compose.

To use it, you have to have Docker Compose as well as VS Code with its "Remote - Containers"
extension (`ms-vscode-remote.remote-containers`) installed.
Then open this repository in VS Code and run the command
`Remote-Containers: Reopen in Container` from the VS Code "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant VS Code extensions pre-installed
- pre-configured linting and auto-formatting
- a pre-configured debugger
- automatic license-header insertion

Moreover, inside the devcontainer, a convenience commands `dev_install` is available.
It installs the service with all development dependencies, installs pre-commit.

The installation is performed automatically when you build the devcontainer. However,
if you update dependencies in the [`./pyproject.toml`](./pyproject.toml) or the
[`./requirements-dev.txt`](./requirements-dev.txt), please run it again.

## License

This repository is free to use and modify according to the
[Apache 2.0 License](./LICENSE).

## README Generation

This README file is auto-generated, please see [`readme_generation.md`](./readme_generation.md)
for details.
