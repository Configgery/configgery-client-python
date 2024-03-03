# Configgery Python client

## Install

```console
$ pip install configgery-client
```

## Getting Started

This library allows you to fetch the latest set of configurations for your client. 

First, add some configurations to a Client Group [configgery.com](configgery.com)
Then, use this library to fetch those same configurations.

Once a client has identified itself, you will see it in the client list at <a>configgery.com/<org_name>/clients/</a>.

### Fetching configurations
```python
from configgery.client import Client

client = Client(API_KEY, "/path/to/store/configurations")
client.download_configurations()
```

### Checking if up-to-date

```python
from configgery.client import Client

client = Client(SDK_KEY, "/path/to/store/configurations")
client.identify("my_client_name")
client.check_latest()
print(client.is_download_needed())
```

### Using a configuration

```python
from configgery.client import Client

client = Client(SDK_KEY, "/path/to/store/configurations")
client.identify("my_client_name")
success, data = client.get_configuration('myconfiguration.json')

if success:
    print(data)
else:
    print('Could not find configuration')
```

### Updating state

```python
from configgery.client import Client, ClientState

client = Client(SDK_KEY, "/path/to/store/configurations")
client.identify("my_client_name")
client.download_configurations()
client.update_state(ClientState.Configurations_Applied)

if device_happy():  # your own check
    client.update_state(ClientState.Upvote)
else:
    client.update_state(ClientState.Downvote)
```

