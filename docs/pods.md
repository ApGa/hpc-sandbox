# Pods

A pod runs several services that share one network, like a docker-compose project: an agent's
sandbox next to the databases, tool servers or graders it reaches by name on fixed ports. Each
service is a sandbox, with its own image, root filesystem and processes. The pod gives the services
a network of their own: nothing outside the pod can reach it, and from it the services reach only
each other and the host services you relay into it, unless you give the pod egress to other hosts.
Both providers run pods (their `capabilities` include `pods`), with the same API.

## An example

A web server and a client in one pod. The client reaches the server by its service name, `web`:

```python
import asyncio

from hpc_sandbox import HealthCheck, PodSpec, SandboxSpec, ServiceSpec, SingularityProvider

IMAGE = "docker://python:3.12-slim"
CHECK = "import urllib.request; urllib.request.urlopen('http://localhost:8000/')"
FETCH = "import urllib.request; print(urllib.request.urlopen('http://web:8000/').status)"

WEB = ServiceSpec(
    "web",
    SandboxSpec(image=IMAGE, workdir="/srv", env={"PYTHONUNBUFFERED": "1"}),
    command=["python", "-m", "http.server", "8000"],
    healthcheck=HealthCheck(
        ["CMD", "python", "-c", CHECK], interval_s=10, start_period_s=60, start_interval_s=0.5
    ),
)


async def main() -> None:
    provider = SingularityProvider()  # EnrootProvider() on a site with enroot
    pod = await provider.start_pod(PodSpec(labels={"run": "example"}))
    async with pod:  # stopped when the block exits
        await pod.start_service(WEB, wait="healthy")  # returns once a health check passed
        client = await pod.start_service(ServiceSpec("client", SandboxSpec(image=IMAGE)))
        result = await client.exec(["python", "-c", FETCH])
        print(result.stdout, end="")
        print(await pod.health("web"))
        print(await pod.logs("web"), end="")
    await provider.wait_for_deletions()  # the pod's files are deleted in the background


asyncio.run(main())
```

Run it on a compute node, inside a batch job ([Getting started](getting-started.md)). It prints the
status of the client's request, the health of `web`, and what `web` printed: its start, then the
requests of its health check and of the client.

```text
200
healthy
Serving HTTP on 0.0.0.0 port 8000 (http://0.0.0.0:8000/) ...
127.0.0.1 - - [17/Oct/2026 19:42:17] "GET / HTTP/1.1" 200 -
127.0.0.1 - - [17/Oct/2026 19:42:18] "GET / HTTP/1.1" 200 -
```

- `start_pod()` starts an empty pod: the process that holds its network ([The holder](#the-holder)).
  Its `labels` let `reap()` and `hpc-sandbox janitor --label run=example` select it.
- `web` runs a command. `start_service(WEB, wait="healthy")` returns once its health check passed
  ([Health checks](#health-checks)).
- `client` has no command: it stays up, idle, for `exec`, like a sandbox.
- `async with pod:` stops the pod and its services when the block exits, however it exits, and
  `wait_for_deletions()` waits until their files are deleted ([Lifetime](#lifetime)).

Pods take little time to start. In a job with 8 CPU cores on a node with a local NVMe disk, with
enroot 3.5 and the image already staged on the node, a pod started in about 0.1 s and a service of
`alpine:3.20` in about 0.2 s. Starting a service includes extracting its image, so larger images
take longer.

## Starting a pod

`await provider.start_pod(spec)` starts a pod for a `PodSpec` and returns it, a `SessionPod`:

| Field | Default | Meaning |
|---|---|---|
| `network` | `"none"` | A `PodNetwork`. `"none"`: the services reach each other and the upstreams, and nothing else. `"egress"`: they also reach other hosts ([Egress](#egress)). |
| `upstreams` | `{}` | Host services the pod may reach: names mapped to Unix sockets on the host, given as `"unix:///absolute/path"` ([Upstreams](#upstreams)). |
| `upstream_ports` | `{}` | The port of an upstream inside the pod, by name. The others get 18080, 18081 and so on, in the order of `upstreams`, skipping the ports given here. |
| `labels` | `{}` | String pairs that `reap()` and `hpc-sandbox janitor --label` select pods by. |
| `users` | `"single"` | A `PodUsers`: what the pod's user namespace maps. `"single"`: your uid and gid, as root. `"subids"`: your subordinate ids too, for services that switch users, with Singularity CE and Apptainer ([Users](#users)). |

`start_pod()` raises `ValueError` for an invalid spec before anything starts: an unknown network or
`users`, `users="subids"` with enroot, whose services cannot switch users, an upstream name that
does not match `[A-Za-z_][A-Za-z0-9_.-]*`, a socket that is not an absolute `unix://` path shorter
than 108 bytes, a port outside 1 to 65535 or used twice, or `upstream_ports` naming an unknown
upstream. It raises `SandboxError` when `nsenter` is missing, for a pod with egress when no
slirp4netns can be found, and for a pod with `users="subids"` when your user has no subordinate ids
or `newuidmap` or `newgidmap` is missing, and `SandboxStartError` when the pod fails to start, after
it stopped what it started.

| Member | What it does |
|---|---|
| `id` | `hpcsb-` and 32 hexadecimal digits. |
| `await start_service(spec, wait="started")` | Starts a service and returns it ([Services](#services)). |
| `services` | The services that started, by name. |
| `routes` | Where each upstream is, inside the pod: `{"api": "http://127.0.0.1:18080"}`. |
| `network` | `"egress"` while the pod has egress, else `"none"`. |
| `await health(name)` | `"starting"`, `"healthy"`, `"unhealthy"` or `"exited"` ([Health checks](#health-checks)). |
| `await logs(name)` | The tail of what the service's command printed, stdout and stderr together, up to 1 MiB. |
| `await set_network(mode)` | Switches between `"none"` and `"egress"` ([Egress](#egress)). |
| `await stop()` | Stops the services and the pod, and deletes its files in the background ([Lifetime](#lifetime)). |

`async with pod:`, or `async with await provider.start_pod(spec) as pod:`, stops the pod when the
block exits. `provider.pods` holds the pods that a provider started and has not stopped.
`start_service()`, `health()`, `logs()` and `set_network()` raise `SandboxError` once the pod is
stopped, and `SandboxLostError` once its holder died; `health()` and `logs()` raise `KeyError` for
a name that is not one of the pod's services. Code typed against the `PodProvider` and `Pod`
protocols works with either provider ([Protocols](python-api.md#protocols)).

### The holder

The holder is a small process that owns the pod's user and network namespaces:

- Its user namespace maps your uid and gid to root, with `setgroups` denied, or, with
  `users="subids"`, your subordinate ids too ([Subordinate ids](#subordinate-ids)). Its network
  namespace has only a loopback interface, which it brings up.
- It lets the pod's users bind ports below 1024, and every group it maps send ICMP echo requests
  (`ping`), as Docker does for containers: the services run in nested user namespaces, which hold no
  capability over this network.
- It relays the upstreams ([Upstreams](#upstreams)) and, while the pod has egress, holds the pipe
  whose closing ends slirp4netns ([Egress](#egress)).
- The kernel kills it when the process that started the pod exits, however that process exits.

The services join the holder's namespaces. Once the holder is gone, calls on the pod and commands in
its services raise `SandboxLostError`.

## Upstreams

An upstream is a service on the host, such as a model gateway or an API proxy, that code in the pod
may use without the pod getting the host's network. You give it as a Unix socket, and the holder
relays it: it listens on the upstream's port at `127.0.0.1` and `::1` inside the pod (only at
`127.0.0.1` where IPv6 is not available), and forwards each connection to the socket on the host.
`pod.routes` has the URL of each upstream:

```python
import asyncio
import tempfile
from pathlib import Path

from hpc_sandbox import PodSpec, SandboxSpec, ServiceSpec, SingularityProvider

FETCH = (
    "import os, urllib.request; print(urllib.request.urlopen(os.environ['API']).read().decode())"
)


async def answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await reader.readuntil(b"\r\n\r\n")  # the request's headers
    writer.write(b"HTTP/1.0 200 OK\r\n\r\nhello from the host")
    await writer.drain()
    writer.close()


async def main() -> None:
    socket_path = Path(tempfile.mkdtemp()) / "api.sock"
    server = await asyncio.start_unix_server(answer, path=socket_path)
    provider = SingularityProvider()  # EnrootProvider() on a site with enroot
    pod = await provider.start_pod(PodSpec(upstreams={"api": f"unix://{socket_path}"}))
    async with pod:
        print(pod.routes)
        spec = SandboxSpec(image="docker://python:3.12-slim", env={"API": pod.routes["api"]})
        service = await pod.start_service(ServiceSpec("main", spec))
        result = await service.exec(["python", "-c", FETCH])
        print(result.stdout, end="")
    server.close()
    await provider.wait_for_deletions()


asyncio.run(main())
```

```text
{'api': 'http://127.0.0.1:18080'}
hello from the host
```

- The holder binds the ports before any service starts, without `SO_REUSEPORT`, so no service can
  take one over: a service that tries to listen on it finds it in use.
- A relayed connection carries any TCP stream, HTTP or not. Half-closed connections and long
  streams pass through.
- The holder connects to the socket for each connection, as your uid, so the socket must exist when
  a service connects, not when the pod starts. When the connection fails, the relay closes the
  service's side of it.
- Nothing outside the pod can reach these ports. Without egress, the upstreams are the only host
  services the pod reaches.

## Services

`await pod.start_service(spec, wait="started")` starts a service for a `ServiceSpec`, and returns
it: a `SingularityService` or an `EnrootService`, both `PodService` sandboxes with `exec`, `upload`
and `download` like any other ([Python API](python-api.md)).

| Field | Default | Meaning |
|---|---|---|
| `name` | required | The service's name and hostname, which every service of the pod resolves to 127.0.0.1: a letter or digit, then up to 62 letters, digits, `_`, `.` or `-`. |
| `sandbox` | required | A `SandboxSpec`: the image, `workdir`, `env`, `mounts` and `memory_mb`. Its `network` is ignored: services share the pod's. `cpus` is advisory. |
| `command` | `None` | The service's command, a full argv list. Without one, the service stays up, idle, for `exec`, like a sandbox. |
| `healthcheck` | `None` | A `HealthCheck` ([Health checks](#health-checks)). |
| `shm_mb` | `None` | The size of the service's `/dev/shm`, in MiB; 64 when `None`, as with Docker. |
| `ulimits` | `{}` | Resource limits by their docker-compose names, such as `nofile`, `stack` or `core`, each `(soft, hard)`, with `-1` for unlimited. |
| `run_as` | `"root"` | A `ServiceRunAs`: who the command, and the commands entered into the service, run as: `"root"`, `"caller"` or `"subids"` ([Users](#users)). |

With `wait="started"`, `start_service()` returns once the service's container runs; with
`wait="healthy"`, once its health check passed too, which for a service without one is at once. It
raises `SandboxStartError` when the container exits before it is up, or is not up within the
provider's `start_timeout_s` (1800 seconds by default), and, with `wait="healthy"`, when the service
turns unhealthy or its command exits before a check passed, or no check passes within
`start_timeout_s`. A command that exits after `start_service()` returned does not undo the start:
`health()` then says `exited`. A failed or cancelled start stops the service and leaves nothing
behind, and a start that the pod's stop interrupts raises `SandboxStartError`.
`start_service()` raises `ValueError` before anything starts for a name that is invalid or taken,
an empty command, a `shm_mb` that is not positive, an unknown ulimit, a soft limit above its hard
limit, an invalid health check, a `run_as` that is unknown or that the runtime cannot give, or
`"subids"` in a pod without `users="subids"`, or a `sandbox` spec that `start()` would refuse.

Each service gets:

- a root filesystem of its own, extracted from the image, and private `/tmp` and `/var/tmp`;
- the pod's user and network namespaces, and mount, PID, IPC and UTS namespaces of its own;
- as PID 1, a small init that sets the hostname (the service's name), the `ulimits`, within the hard
  limits of the process that started the pod, and a `/dev/shm` tmpfs of `shm_mb`; runs the runtime,
  and so the command, as its child; passes SIGTERM, SIGINT and SIGHUP on to it; reaps orphans; and
  exits with the command's status;
- a nested user namespace, where it runs as root, which is your uid on the host, or as your uid
  itself ([Users](#users)): it holds no capability over the pod's network, so code in a service can
  neither reconfigure that network nor open raw sockets to watch the other services' traffic;
- for its command, the image's environment, `HOME=/root`, then the spec's `env`, passed on its stdin
  and never on a command line; its `workdir`, created when missing, as the working directory; and
  `/dev/null` as stdin;
- a log: the command's stdout and stderr go through a pipe into a file that the service cannot
  truncate and that its own file size limit does not bind, and `logs()` returns its tail;
- the memory watchdog, when its spec sets `memory_mb`, as a sandbox does
  ([Memory limits](python-api.md#memory-limits)).

`ulimits` accepts `nproc` and `locks` but does not apply them: the kernel counts processes per user
namespace, so a service's `nproc` would count every process of the pod (under Docker it does not
bind root either), and `locks` has had no effect since Linux 2.4. A service without a command runs
an idle loop, which needs `sleep` in the image. Services cannot be snapshotted yet, and get no GPUs
([Roadmap](roadmap.md)).

## Users

A service's `run_as` says who its command, and the commands entered into it, run as. Root alone is
not always enough: the official images of databases such as PostgreSQL and Redis start as root,
give their data directories to a user of their own with `chown`, and switch to it with `gosu` or
`su-exec`. In a namespace that maps no other user, that `chown` fails with `Invalid argument`, and
they exit at once.

| `run_as` | The command runs as | It can switch to | Needs | Runtimes |
|---|---|---|---|---|
| `"root"`, the default | root, which is your uid on the host | no other user | nothing | both |
| `"caller"` | your own uid and gid, without capabilities | no other user | nothing | both |
| `"subids"` | root, which is your uid on the host | uids and gids 1 to N: your subordinate ids | a pod with `users="subids"` ([Subordinate ids](#subordinate-ids)) | Singularity CE and Apptainer |

- `"caller"` suits the images that run as any user, as `docker run --user` asks of them: the
  database images skip their `chown` and their switch when they do not start as root. Images that
  insist on root fail.
- `"subids"` gives what Docker gives: a root that can become other users (`su`, `setpriv`, `gosu`)
  and give them files (`chown`). A process that became another user cannot become root again.

In every mode, the image's files belong to your uid on the host, as hpc-sandbox extracts them, so a
service sees them as root's, or with `"caller"` as its own. Commands get `HOME=/root` unless `env`
sets it. The service's owner record says how it ran: `run_as`, and `remap_root` and
`container_uid` as for a sandbox, `false` and your uid with `"caller"`.

A PostgreSQL server with its image's own entrypoint and command:

```python
import asyncio

from hpc_sandbox import HealthCheck, PodSpec, SandboxSpec, ServiceSpec, SingularityProvider

IMAGE = "docker://postgres:16-alpine"
POSTGRES = ServiceSpec(
    "postgres",
    SandboxSpec(image=IMAGE, workdir="/", env={"POSTGRES_PASSWORD": "example"}),
    command=["docker-entrypoint.sh", "postgres"],  # the image's ENTRYPOINT and CMD
    healthcheck=HealthCheck(
        ["CMD", "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
        interval_s=10,
        start_period_s=60,
        start_interval_s=0.5,
    ),
    run_as="subids",  # "caller" where you have no subordinate ids, with PodSpec()
)
QUERY = ["psql", "-h", "postgres", "-U", "postgres", "-tAc", "select 6 * 7"]


async def main() -> None:
    provider = SingularityProvider()
    async with await provider.start_pod(PodSpec(users="subids")) as pod:
        await pod.start_service(POSTGRES, wait="healthy")
        client = await pod.start_service(ServiceSpec("client", SandboxSpec(image=IMAGE)))
        result = await client.exec(QUERY, env={"PGPASSWORD": "example"})
        print(result.stdout, end="")
    await provider.wait_for_deletions()


asyncio.run(main())
```

```text
42
```

### Subordinate ids

`PodSpec(users="subids")` maps more than your uid into the pod's user namespace: root to your uid
and gid, as always, and uids and gids 1 to N to the first range that `/etc/subuid` and
`/etc/subgid` give your user, by name or by uid, such as `alice:100000:65536`. `newuidmap` and
`newgidmap`, the setuid helpers of shadow-utils that rootless Docker, Podman and Singularity's
`--fakeroot` use, write that map, and leave `setgroups` allowed. They do it once per process: the
first such pod maps a user namespace that the pod's, and those of every later pod of the process
and of the processes it forks, are made in, mapped as it is. That first pod can take seconds to
start where the ranges of many users in `/etc/subuid` overlap: shadow-utils looks up the owner of
each range that holds an id it checks.
`hpc-sandbox doctor` shows your ranges on its `pods: subordinate ids` line
([Command line](cli.md#doctor)). Only Singularity CE and Apptainer start such pods: enroot maps one
id into a container, so `EnrootProvider.start_pod()` raises `ValueError` for them.

`await provider.map_subordinate_ids()` makes that mapping at once, rather than at the first such
pod, and raises what `start_pod()` would raise for such a pod before it starts anything. A fork
server calls it before it forks its workers, whose pods then start without mapping: they inherit
the mapping through an open descriptor. A process that closed that descriptor maps again at its
next such pod.

Such a pod runs services of every `run_as`. The namespace of a `"subids"` service maps every id
that the pod's maps, each to itself, so its root can switch to any of them, and drops your
supplementary groups, as root in a Docker container has none of the host's. The other services map
root alone or your uid alone, as in any pod. A working directory that a `"subids"` service's image
lacks is made with mode 755 whatever your umask, as Docker's `-w` makes it, so that the users it
switches to can enter it.

What a service writes as another user belongs, on the host, to one of your subordinate ids: you can
neither change nor delete it, nor a directory that holds it. So before a pod's files are deleted,
hpc-sandbox gives them back to your uid: as root of a user namespace that maps your subordinate ids
as the pod's does, `find -xdev ... -exec chown -h 0:0` makes root, your uid, the owner of everything
another id owns, never following a link or crossing into another filesystem. This runs:

- at `stop()`, once the services have stopped, in the holder's namespace, over the pod's whole
  session directory (`pod.path`), so directories you create there and bind into the services are
  covered too;
- when a `"subids"` service stops before its pod (a start that fails, or its own `stop()`), over
  that service's files;
- when a janitor sweeps an abandoned pod whose owner record says `users: "subids"`, once it has
  stopped what still ran, in its process's namespace of subordinate ids.

When it fails, hpc-sandbox logs a warning, and the deletion refuses what is left, naming it. What a
service writes as another user into a writable bind from outside `pod.path` stays your subordinate
ids'.

## Names

Each service has an `/etc/hosts` of its own, bound read-only into it. It maps `localhost` to
127.0.0.1 and ::1, and the service's own name and the names of the other services to 127.0.0.1. A
service's name is added to the other services' files when it starts, and taken out again if its
start fails. Since all services share one loopback interface, a service that listens on a port is
reachable from every service of the pod as `name:port`, `localhost:port` or `127.0.0.1:port`.
Unlike docker-compose, which gives each service an address of its own, a pod has one address for
all its services: give them different ports.

`/etc/resolv.conf` names `10.0.2.3`, slirp4netns's DNS forwarder, which answers while the pod has
egress. The names of the services resolve through `/etc/hosts` either way.

On images based on Debian (those with `/etc/debian_version`), hpc-sandbox lets apt run as root
(`APT::Sandbox::User "root"`): with only one user mapped, apt could not switch to its `_apt` user.

## Commands

`exec`, `upload`, `download` and health checks enter a service in two steps: the pod's user and
network namespaces with `nsenter`, then the container's own namespaces and root directory. They run
like the service's command, as the user its `run_as` gives ([Users](#users)), with its `ulimits`
(applied with `prlimit`). `exec` follows the rules of every sandbox: the same environment, timeouts, exit codes
and errors ([Running commands](python-api.md#running-commands)).

Once a service's command has exited, its container is gone: `exec` and transfers raise
`SandboxLostError`, while `health()` and `logs()` still answer. A service without a command runs
commands until the pod stops.

## Health checks

`HealthCheck` follows docker-compose's healthcheck:

| Field | Default | Meaning |
|---|---|---|
| `test` | required | `["CMD", program, arg, ...]` runs the program; `["CMD-SHELL", command]`, or a plain string, runs a shell command with `/bin/sh -c`; `["NONE"]` turns the check off. |
| `interval_s` | `30.0` | Seconds between two checks. |
| `timeout_s` | `30.0` | How long one check may run; a check that times out fails. |
| `retries` | `3` | How many failures in a row make the service unhealthy. |
| `start_period_s` | `0.0` | Seconds after the start during which failures do not count, until a check passes. |
| `start_interval_s` | `5.0` | Seconds between two checks during the start period, until a check passes. |

A check runs in the service like `exec`, and passes when it exits with status 0; a check that cannot
run fails. The first check runs `start_interval_s` after the start when `start_period_s` is set,
and `interval_s` after it otherwise, so give a check a start period to see the service healthy soon
after it starts. Checks run until the service's command exits or the pod stops.
`await pod.health(name)` returns the service's status, a `ServiceHealth`:

| Status | When |
|---|---|
| `starting` | No check has passed yet, and the service is not unhealthy. |
| `healthy` | A check passed, and fewer than `retries` checks have failed in a row since. A service without a health check, or with `["NONE"]`, is healthy as soon as it runs. |
| `unhealthy` | `retries` checks failed in a row, not counting failures within the start period. |
| `exited` | The service's command has exited, whatever its check says. The service's `exit_status` holds the command's status, `128 + N` when signal N killed it. |

`wait="healthy"` waits while the service is `starting`. When it turns `unhealthy` or `exited`
first, or stays `starting` for the provider's `start_timeout_s`, the service is stopped and
`start_service()` raises `SandboxStartError` with the output of the last failed check, or the tail
of the service's log.

## Egress

`network="egress"` in the `PodSpec`, or `await pod.set_network("egress")` later, gives the services
access to other hosts through slirp4netns, which runs with `--disable-host-loopback`,
`--enable-sandbox` and `--enable-seccomp`:

- The services reach other hosts through the gateway `10.0.2.2`, and resolve names through
  `10.0.2.3`. Their connections leave from the node, as connections of your uid.
- They never reach the host's loopback interface: relay the host services they need as
  [upstreams](#upstreams). They do reach what the node's network reaches, including services that
  listen on the node's own network addresses.
- `await pod.set_network("none")` cuts egress again, and `pod.network` says which of the two is on.
  An agent can, for example, install packages with egress and then work, or be graded, without.

The provider runs the first slirp4netns it finds of these:

1. its `slirp4netns` option, a name on `PATH` or a path;
2. `slirp4netns` on `PATH`, if its `--help` lists the options pods need (`--disable-host-loopback`,
   `--enable-sandbox`, `--enable-seccomp`, `--exit-fd` and `--ready-fd`, all in slirp4netns 0.4 and
   later);
3. the static slirp4netns 1.3.6 that `hpc-sandbox fetch-tools` downloads, for x86_64 or aarch64,
   into `$HPC_SANDBOX_TOOLS`, else `${XDG_CACHE_HOME:-~/.cache}/hpc-sandbox/tools`, which is checked
   against its pinned SHA-256 before use.

With none of them, `start_pod()` with egress and `set_network("egress")` raise `SandboxError`,
which names these three ways. Compute nodes often have no internet access: run
`hpc-sandbox fetch-tools` once where there is some, into a directory the nodes share
([Command line](cli.md#fetch-tools)). A slirp4netns from the site's packages gets its
distribution's security fixes; the pinned one gets fixes only when hpc-sandbox moves its pin.

Only the holder keeps slirp4netns's exit pipe open, so slirp4netns ends with the pod, even when the
process that started the pod is killed.

## Lifetime

`await pod.stop()`:

1. cancels the service starts in progress, whose `start_service()` calls raise `SandboxStartError`;
2. stops the health checks, and refuses new commands in the services; commands still running raise
   `SandboxError` when their service stops;
3. when the provider's `service_stop_grace_s` is above 0 (it is 0 by default), sends SIGTERM to the
   commands of the services that have one, through their inits, and waits up to that long for them
   to exit;
4. kills each service's init, which takes every process of the service with it;
5. in a pod with `users="subids"`, gives back to your uid what the services wrote as other users
   ([Subordinate ids](#subordinate-ids));
6. stops slirp4netns and the holder;
7. moves the pod's session directory, which holds the sessions of its services, into the trash, and
   deletes it in the background, as for a sandbox: `await provider.wait_for_deletions()` waits for
   that ([Stopping and deleting](python-api.md#stopping-and-deleting)).

`stop()` is idempotent, and safe to cancel: the teardown runs to its end. When a service cannot be
stopped, it raises `SandboxError` and keeps the pod's files for a janitor, and a later `stop()`
tries again. Services stop with their pod; a command that ends early ends only its service.

Pods die with the process that started them, with either runtime: the kernel kills the holder, and
each service's launcher and init, when that process exits, even by SIGKILL. What remains are files,
which a janitor removes, giving back first what a `users="subids"` pod's other users left: a
provider's first start that finds the janitor lock free, `await provider.reap()`, or
`hpc-sandbox janitor`; the last two take labels to limit the sweep to the pods that carry them ([Reaping and janitors](python-api.md#reaping-and-janitors)). With labels,
`reap()` also stops the provider's own live pods whose labels include them. A pod's files are on the
scratch root, in `pods-singularity/` or `pods-enroot/`
([Where files go](configuration.md#where-files-go)).

## Requirements and site notes

| Requirement | Needed for |
|---|---|
| unprivileged user namespaces, nested: the pod's, and each service's inside it | every pod, with either runtime |
| util-linux `nsenter` | every pod: `exec`, transfers and health checks in its services |
| util-linux `prlimit` | commands in services that set `ulimits` |
| slirp4netns ([Egress](#egress)), and a `/dev/net/tun` that users may open, as it usually is | egress |
| a range of subordinate ids for your user in `/etc/subuid` and `/etc/subgid`, and shadow-utils' `newuidmap` and `newgidmap` (the `uidmap` package on Debian and Ubuntu) | pods with `users="subids"`, with Singularity CE and Apptainer ([Subordinate ids](#subordinate-ids)) |
| `/bin/sh` in the image and, for a service without a command, what a sandbox needs in its image ([Image references](python-api.md#image-references)) | every service |

`hpc-sandbox doctor` shows on its `pods: egress` line which slirp4netns pods would use, and, with
Singularity, on its `pods: subordinate ids` line the ranges that pods with `users="subids"` would
map ([Getting started](getting-started.md#check-the-installation)).

- **Singularity CE and Apptainer.** A service runs `singularity exec --cleanenv --no-eval --writable
  --containall --no-home --no-mount home,cwd,bind-paths` on a root filesystem directory of its own,
  in a nested user namespace that the launcher creates, so the runtime runs unprivileged, with its
  non-setuid starter, even where it is installed setuid; with `run_as="caller"`, that namespace maps
  your uid and gid, and the runtime runs with `--userns` as them. The service's `/tmp`, `/var/tmp`,
  `/dev/shm`, `/etc/hosts` and `/etc/resolv.conf`, and the spec's mounts, are bound into the
  container. Pods never use `--network none`, so `allow_network_none` does not matter to them.
  Services always get the copy layout, whatever `rootfs_layout` says, and with
  `rootfs_templates=True` they copy the node's template of their image
  ([Singularity CE and Apptainer](singularity.md)).
- **enroot.** A service runs `enroot start --rw` on an `enroot create` extraction of its own, with
  `--root` unless the provider has `remap_root=False`, and with the provider's `extra_start_args`.
  Root lives in enroot's own user namespace, nested in the pod's. With `run_as="caller"`, the
  launcher first nests a user namespace that maps your uid and gid to the pod's root, and enroot
  runs without `--root` in it. enroot maps one id into a container, so it refuses
  `run_as="subids"`, and pods with `users="subids"`. The same files are bound into the container as
  with Singularity, and services get no GPUs, whatever `gpus` says
  ([enroot](enroot.md#pod-services)).

## Next steps

- [Python API](python-api.md): what every sandbox, and so every service, does.
- [Getting started](getting-started.md): run pods inside Slurm and PBS batch jobs.
- [Security model](security.md): what a pod's network keeps apart, and what it does not.
- [Command line](cli.md): `fetch-tools` for egress, and `janitor` for the files of dead pods.
- [Roadmap](roadmap.md): what pods do not do yet.
