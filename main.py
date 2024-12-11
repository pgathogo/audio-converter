from audio_convert import AudioConverter
def get_config(config_file: str) -> dict:
    config = {}
    # Read ini file to get dbf_folder, audio_folder, output_folder and make a dictionary
    with open(config_file, "r") as f:
        config_data = f.read().split("\n")
        for line in config_data:
            if line.startswith("#") or line == "": # Skip comments and empty lines
                continue
            key, value = line.split("=") # Split the line into key and value
            print(f"{key}: {value}")
            config[key] = value

    return config

if __name__ == "__main__":
    config_ini = "config.ini"
    config = get_config(config_ini)
    audio_converter = AudioConverter(**config)
    audio_converter.convert()   