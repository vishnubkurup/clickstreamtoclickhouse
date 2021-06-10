def flatten_json(y):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(
                    x[a],
                    clean_event_key(name) + clean_event_key(a) + "_",
                )
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, clean_event_key(name) + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def clean_event_key(key):
    return key.strip().replace(" ", "").replace(":", "_").replace("-", "_")
