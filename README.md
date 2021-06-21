# Artillery.io SocketCluster Engine

Load Test [SocketCluster](https://socketcluster.io/) with [Artillery](https://artillery.io).

Tested with SocketCluster v16.0.1.

## Usage

### Install the plugin

```
# If Artillery is installed globally:
npm install -g artillery-engine-socketcluster
```

### Define a scenario

```yaml
config:
  target: "localhost"
  socketcluster:
    autoCreate: false
  phases:
    - duration: 10
      arrivalRate: 5
  engines:
    socketcluster: {}
scenarios:
  - engine: "socketcluster"
    flow:
      - create:
          path: /socketcluster/
          port: 443
      - invoke:
          procedureName: "myTestProcedure"
          match:
            json: "$.myTestData"
            value: "{{ myExpectedValue }}"
      - think: 10
      - subscribe:
          channel: "myTestChannel"
          match:
            json: "$.myTestData"
            value: "{{ myExpectedValue }}"
```

All of SocketCluster [AGClientSocket](https://socketcluster.io/docs/api-ag-client-socket/)'s methods can be used in a scenario's flow.
The parameters have the same names (case-sensitive) as those listed in the [official SC documentation](https://socketcluster.io/docs/api-ag-client-socket/#methods).

In addition, if autoCreate is set to false in the engine's configs (see example above), the method "create" can be used to manually create the AGClientSocket. This invokes the [socketClusterClient.create](https://socketcluster.io/docs/api-socket-cluster-client/#methods) method with the given parameters, otherwise the client is created with the parameters set under config.socketcluster.

### Run the scenario

```
artillery run my-scenario.yml
```


### License

[MPL 2.0](https://www.mozilla.org/en-US/MPL/2.0/)


