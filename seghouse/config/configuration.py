from dataclasses import dataclass
from typing import List

import humps
import yaml


@dataclass(frozen=True, eq=True)
class App:
    """Class for recording app names with write keys."""

    write_key: str
    name: str

    def schema(self):
        return humps.decamelize(self.name)


yaml.add_path_resolver("!app", ["App"], dict)


@dataclass(frozen=True, eq=True)
class AppConf:
    """Top level configuration class"""

    apps: List[App]
    warehouses: List[dict]
    skip_fields: List[str]
    extra_timestamps: dict


def from_yaml(file_path: str):
    apps = set()
    skip_fields = []
    with open(file_path) as file:
        resolved_conf = yaml.load(file, Loader=yaml.FullLoader)
        for app_dict in resolved_conf.get("apps", []):
            apps.add(App(app_dict["write_key"], app_dict["name"]))
        for f in resolved_conf.get("skip_fields", []):
            skip_fields.append(f)
        extra_timestamps = resolved_conf.get("extra_timestamps", {})
    return AppConf(
        apps=list(apps), warehouses=resolved_conf["warehouses"], skip_fields=skip_fields,
        extra_timestamps=extra_timestamps
    )
