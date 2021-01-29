# What is this?
This package contains dockerregistrypusher CLI that allows to push image packed as tar (usually from docker save command) to a docker registry
This project was forked from [Adam Raźniewski's dockerregistrypusher](https://github.com/Razikus/dockerregistrypusher) but with changes and adjustments to iguazio's needs as a CLI<br>
All rights reserved to the original author [Adam](https://github.com/Razikus)

# Why?
To push tar-packed image archives (created by `docker save`) to registries without going through (and taxing) docker-daemon

Usage of CLI:

# installation

Install and create a symlink at `/usr/local/bin/dockerregistrypusher` (requires sudo)
```shell
./install
```

Or, install without symlink creation (no elevated permissions needed)
```shell
./install --no-link
```

# Running the CLI

CLI structure
```shell
dockerregistrypusher [options] {TAR_PATH} {REGISTRY_URL}
```

For further help (duh)
```shell
dockerregistrypusher --help
```


# Development
To be able to run linting / formatting and other `make` goodness, install with dev requirements
```shell
make install
```

# License
Free to use (MIT)
