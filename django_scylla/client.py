from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = "cqlsh"

    @classmethod
    def settings_to_cmd_args_env(cls, settings_dict, parameters):
        args = [cls.executable_name]
        if settings_dict["HOST"]:
            args.extend([settings_dict["HOST"].split(",")[0]])
        if settings_dict["PORT"]:
            args.extend([str(settings_dict["PORT"])])
        if settings_dict["USER"]:
            args += ["-u", settings_dict["USER"]]
        if settings_dict["PASSWORD"]:
            args += ["-p", settings_dict["PASSWORD"]]
        args += ["-k", settings_dict["NAME"]]
        args.extend(parameters)

        env = {}

        return args, (env or None)
