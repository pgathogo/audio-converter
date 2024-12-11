import os
import json
from dbf_reader import get_data

from subprocess import PIPE, run

class AudioConverter:
    def __init__(self, **kwargs):
        self.dbf_folder = kwargs.get("dbf_folder", "dbf/")
        self.audio_folder = kwargs.get("audio_folder", "audio/")
        self.output_folder = kwargs.get("output_folder", "output/")
        self.keep_converted = kwargs.get("keep_converted", False)

    def convert(self):
        # Loop through a folder and read all files with extension .dbf
        print(f"Reading data from: {self.dbf_folder}")
        dbf_files = [f for f in os.listdir(self.dbf_folder+"/") if f.endswith('.DBF')]
        print(f"{len(dbf_files)} files found")
        for dbf in dbf_files:
            print(f"Reading data from: {dbf}")
            data = get_data(self.dbf_folder+"/"+dbf)
            print(f"{len(data)} records found")
            self.write_data(data, dbf)
            print(f"Converting audio files to ogg")
            self.convert_audio(data, dbf)

    def write_data(self, data: list, dbf: str):
        # Write data to a json file, remoce extension .DBF
        dbf = dbf[:-4]
        print(f"Writing data to: {self.dbf_folder}/{dbf}.json")
        with open(f"{self.dbf_folder}/{dbf}.json", "w") as f:
            json.dump(data, f, indent=4)

    def convert_audio(self, data: list, dbf: str):
        # Convert audio files from MTS to ogg
        missing_files = []
        converted_files = []
        dbf = dbf[:-4]
        for i, record in enumerate(data):
            input_file = f"{self.audio_folder}/{dbf}/{record['audio_file']}"

            # Check if audio file exists
            if not os.path.exists(f"{input_file}"):
                missing_files.append(record)
                continue

            # Check if output folder exists
            if not os.path.exists(self.output_folder):
                os.makedirs(self.output_folder)

            # Check if ffmpeg is installed
            if not os.path.exists("ffmpeg.exe"):
                raise Exception("ffmpeg is not installed")

            output_file = f"{self.output_folder}/{dbf}{record['code']}.ogg"

            if self.keep_converted:
                if os.path.exists(output_file):
                    continue

            print(f"Converting {input_file} to {output_file}")

            os.system(f"ffmpeg -y -i {input_file} -c:a libvorbis -q:a 4 -vsync 2 {output_file}")

            duration = self.probe_audio_duration(output_file)

            record["duration"] = duration * 1000 # milliseconds
            converted_files.append(record)

        
        # Write converted files to a json file
        # Remove extension .DBF
        with open(f"{self.dbf_folder}/{dbf}_converted.json", "w") as f:
            json.dump(converted_files, f, indent=4) 

        
        # Write missing files to a json file
        # Remove extension .DBF
        with open(f"{self.dbf_folder}/{dbf}_missing.json", "w") as f:
            json.dump(missing_files, f, indent=4)

    def probe_audio_duration(self, audio_file: str)-> float:
        # Get audio duration in seconds
        result = run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", audio_file], stdout=PIPE, stderr=PIPE)
        return float(result.stdout.decode("utf-8"))
