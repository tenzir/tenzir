# nics

Shows a snapshot of available network interfaces.

```tql
nics
```

## Description

The `nics` operator shows a snapshot of all available network interfaces.

## Schemas

Tenzir emits network interface card information with the following schema.

### `tenzir.nic`

Contains detailed information about the network interface.

|Field|Type|Description|
|:-|:-|:-|
|`name`|`string`|The name of the network interface.|
|`description`|`string`|A brief note or explanation about the network interface.|
|`addresses`|`list`|A list of IP addresses assigned to the network interface.|
|`loopback`|`bool`|Indicates if the network interface is a loopback interface.|
|`up`|`bool`|Indicates if the network interface is up and can transmit data.|
|`running`|`bool`|Indicates if the network interface is running and operational.|
|`wireless`|`bool`|Indicates if the network interface is a wireless interface.|
|`status`|`record`|A record containing detailed status information about the network interface.|

The record `status` has the following schema:

|Field|Type|Description|
|:-|:-|:-|
|`unknown`|`bool`|Indicates if the network interface status is unknown.|
|`connected`|`bool`|Indicates if the network interface is connected.|
|`disconnected`|`bool`|Indicates if the network interface is disconnected.|
|`not_applicable`|`bool`|Indicates if the network interface is not applicable.|

## Examples

List all connected network interfaces.

```tql
nics
where status.connected
```
