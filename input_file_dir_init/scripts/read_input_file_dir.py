import os

def load_parameters(file_path):
    """
    Reads a configuration file and returns parameters as a dictionary.
    :param file_path: Path to the configuration text file.
    :return: Dictionary of parameters.
    """
    if not os.path.exists(file_path):
        raise IOError("Configuration file not found: %s" % file_path)

    parameters = {}
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()

            # Ignore empty lines and full-line comments
            if not line or line.startswith("#"):
                continue

            # Remove inline comments
            if "#" in line:
                line = line.split("#", 1)[0].strip()

            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()

                # Type conversion
                try:
                    if "." in value:
                        value = float(value)
                    else:
                        value = int(value) if value.isdigit() else value
                except ValueError:
                    pass

                parameters[key] = value

    return parameters
