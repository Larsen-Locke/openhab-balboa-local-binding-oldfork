# Balboa Binding

This binding integrates Balboa hot tub / spa control units that are connected to the local network
via a Balboa Wi-Fi module (BWA / WiFi Module 50350 and compatible). It connects directly to the unit
over TCP/IP and exposes its status (temperature, pumps, lights, blower, mister, filter cycle, heat
mode, ...) as openHAB channels.

## Supported Things

There is exactly one thing type:

| Thing        | Thing Type ID | Description                             |
|--------------|----------------|------------------------------------------|
| Balboa IP Unit | `balboa-ip`  | A Balboa control unit reachable over IP |

The set of channels a thing actually gets depends on the equipment configuration reported by the
control unit itself (which pumps/lights/blower/mister are installed, one- or two-speed) - the binding
reads this out automatically after connecting and builds the channel list accordingly, so different
tubs will show a different subset of the channels listed below.

## Discovery

This binding does not support auto-discovery. Things must be added manually with the host/IP address
of the Balboa Wi-Fi module.

## Thing Configuration

| Parameter          | Type    | Required | Default | Description                                                        |
|---------------------|---------|----------|---------|----------------------------------------------------------------------|
| `host`              | text    | yes      | -       | Hostname or IP address of the Balboa Wi-Fi module                   |
| `port`              | integer | yes      | 4257    | TCP port of the Balboa Wi-Fi module                                  |
| `reconnectInterval` | integer | yes      | 30      | Seconds to wait before attempting to reconnect after a disconnect    |
| `pollingInterval`   | integer | yes      | 60      | Seconds between polling requests sent to keep the connection alive   |

### `balboa.things` Example

```java
Thing balboa:balboa-ip:mySpa "My Spa" [ host="192.168.1.50", port=4257, reconnectInterval=30, pollingInterval=60 ]
```

## Channels

Channels marked _(dynamic)_ are only present if the corresponding equipment is configured on the unit.

| Channel               | Type               | Read/Write | Description                                              |
|------------------------|---------------------|:----------:|------------------------------------------------------------|
| `current-temperature`  | `Number:Temperature` |     R      | Current water temperature                                 |
| `target-temperature`   | `Number:Temperature` |    R/W     | Target water temperature                                  |
| `temperature-scale`    | `String`             |    R/W     | Display scale: `C` or `F`                                  |
| `temperature-range`    | `String`             |    R/W     | Temperature range: `LOW` or `HIGH`                          |
| `heat-mode`            | `String`             |    R/W     | Heat mode: `READY`, `REST`, `READY_IN_REST`                 |
| `filter`               | `String`             |     R      | Filter cycle status: `OFF`, `1`, `2`, `1+2`                  |
| `priming`              | `Contact`             |     R      | Priming indicator                                          |
| `circulation`          | `Contact`             |     R      | Circulation pump status                                    |
| `heater`               | `Contact`             |     R      | Heater status                                              |
| `pump-<n>` _(dynamic)_ | `Switch` or `String` |    R/W     | Jet pump `n`: `Switch` if one-speed, `String` (`OFF`/`LOW`/`HIGH`) if two-speed |
| `light-<n>` _(dynamic)_| `Switch` or `String` |    R/W     | Light `n`: `Switch` if one-level, `String` (`OFF`/`LOW`/`HIGH`) if two-level |
| `aux-<n>` _(dynamic)_  | `Switch`             |    R/W     | Auxiliary output `n`                                        |
| `blower` _(dynamic)_   | `Switch` or `String` |    R/W     | Blower: `Switch` if one-speed, `String` (`OFF`/`LOW`/`HIGH`) if two-speed |
| `mister1`/`mister2` _(dynamic)_ | `Switch` or `String` | R/W | Mister: `Switch` if one-speed, `String` (`OFF`/`LOW`/`HIGH`) if two-speed |

## Full Example

`balboa.things`:

```java
Thing balboa:balboa-ip:mySpa "My Spa" [ host="192.168.1.50", port=4257 ]
```

`balboa.items`:

```java
Number:Temperature  Spa_CurrentTemp   "Current Temperature [%.1f %unit%]" { channel="balboa:balboa-ip:mySpa:current-temperature" }
Number:Temperature  Spa_TargetTemp    "Target Temperature [%.1f %unit%]"  { channel="balboa:balboa-ip:mySpa:target-temperature" }
Switch               Spa_Pump1         "Jet Pump 1"                        { channel="balboa:balboa-ip:mySpa:pump-1" }
Switch               Spa_Light1        "Lights"                            { channel="balboa:balboa-ip:mySpa:light-1" }
```
