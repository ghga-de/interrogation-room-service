
[![tests](https://github.com/ghga-de/interrogation-room-service/actions/workflows/tests.yaml/badge.svg)](https://github.com/ghga-de/interrogation-room-service/actions/workflows/tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/interrogation-room-service/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/interrogation-room-service?branch=main)

# Interrogation Room Service

Interrogation Room Service

## Description

<!-- Please provide a short overview of the features of this service.-->

The Interrogation Room Service (IRS) interfaces with the Encryption Key Store Service to process Crypt4GH encrypted files uploaded to the inbox of a local GHGA node.
The IRS splits off the file envelope, computes part checksums over the encrypted file content, validates the checksum over the unencrypted file content (in memory) and initiates transfer of the encrypted file content to its permanent storage.


## Installation

We recommend using the provided Docker container.

A pre-build version is available at [docker hub](https://hub.docker.com/repository/docker/ghga/interrogation-room-service):
```bash
docker pull ghga/interrogation-room-service:0.3.2
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
docker build -t ghga/interrogation-room-service:0.3.2 .
```

For production-ready deployment, we recommend using Kubernetes, however,
for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is preconfigured:
docker run -p 8080:8080 ghga/interrogation-room-service:0.3.2 --help
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


- **`s3_endpoint_url`** *(string)*: URL to the S3 API.


  Examples:

  ```json
  "http://localhost:4566"
  ```


- **`s3_access_key_id`** *(string)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.


  Examples:

  ```json
  "my-access-key-id"
  ```


- **`s3_secret_access_key`** *(string, format: password)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.


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


- **`service_name`** *(string)*: Default: `"interrogation_room"`.

- **`service_instance_id`** *(string)*: A string that uniquely identifies this instance across all instances of this service. A globally unique Kafka client ID will be created by concatenating the service_name and the service_instance_id.


  Examples:

  ```json
  "germany-bw-instance-001"
  ```


- **`kafka_servers`** *(array)*: A list of connection strings to connect to Kafka bootstrap servers.

  - **Items** *(string)*


  Examples:

  ```json
  [
      "localhost:9092"
  ]
  ```


- **`staging_bucket`** *(string)*: Bucket ID representing the staging area for re-encrypted files.


  Examples:

  ```json
  "staging"
  ```


- **`eks_url`** *(string)*: URL pointing to the Encryption Key Store service.


  Examples:

  ```json
  "http://127.0.0.1/eks"
  ```



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
<!-- Please provide an overview of the architecture and design of the code base.
Mention anything that deviates from the standard triple hexagonal architecture and
the corresponding structure. -->

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
