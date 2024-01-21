# Configgery Python client

## Install

```console
$ pip install configgery-client
```

## Getting Started

This library allows you to fetch the latest set of configurations for your device. 

First, create a device at [configgery.com](configgery.com) 
and store the generated private key and certificate in a safe place. 
Then, once you've added configurations to your device's group, 
you can fetch those same configurations.

### Fetching configurations
```python
from configgery.client import Client

client = Client('/path/to/configurations_dir', API_KEY)
client.download_configurations()
```

### Checking if up-to-date

```python
from configgery.client import Client

client = Client('/path/to/configurations_dir', API_KEY)
client.check_latest()
print(client.is_download_needed())
```

### Using a configuration

```python
from configgery.client import Client

client = Client('/path/to/configurations_dir', API_KEY)
success, data = client.get_configuration('myconfiguration.json')

if success:
    print(data)
else:
    print('Could not find configuration')
```

### Updating state
```python
from configgery.client import Client, DeviceState

client = Client('/path/to/configurations_dir', API_KEY)
client.download_configurations()
client.update_state(DeviceState.Configurations_Applied)

if device_happy():  # your own check
    client.update_state(DeviceState.Upvote)
else:
    client.update_state(DeviceState.Downvote)
```

