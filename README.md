# Configgery Python client

## Install

```console
$ pip install configgery-client
```

## Getting Started

This library allows you to fetch the latest set of configurations for your client. 

First, create a client at [configgery.com](configgery.com) 
and store the generated API key in a safe place. 
Then, once you've added configurations to your Client Group, 
you can fetch those same configurations.

### Fetching configurations
```python
from configgery.client import Client

client = Client(API_KEY, "/path/to/store/configurations")
client.download_configurations()
```

### Checking if up-to-date

```python
from configgery.client import Client

client = Client(API_KEY, "/path/to/store/configurations")
client.check_latest()
print(client.is_download_needed())
```

### Using a configuration

```python
from configgery.client import Client

client = Client(API_KEY, "/path/to/store/configurations")
success, data = client.get_configuration('myconfiguration.json')

if success:
    print(data)
else:
    print('Could not find configuration')
```

### Updating state

```python
from configgery.client import Client, ClientState

client = Client(API_KEY, "/path/to/store/configurations")
client.download_configurations()
client.update_state(ClientState.Configurations_Applied)

if device_happy():  # your own check
    client.update_state(ClientState.Upvote)
else:
    client.update_state(ClientState.Downvote)
```

