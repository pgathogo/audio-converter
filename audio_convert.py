import os
import json
from timeit import default_timer as timer
import datetime
from datetime import timedelta

from dbf_reader import get_data

from subprocess import PIPE, run

class AudioConverter:
    def __init__(self, **kwargs):
        self.dbf_folder = kwargs.get("dbf_folder", "dbf/")
        self.audio_folder = kwargs.get("audio_folder", "audio/")
        self.output_folder = kwargs.get("output_folder", "output/")
        self.log_folder = kwargs.get("log_folder", "log/")
        self.artists_file = kwargs.get("artists_file", "artists.txt")

        converted = kwargs.get("keep_converted", "False")
        if converted == "True":
            self.keep_converted = True
        else:
            self.keep_converted = False

        self.artists = self.fetch_artists("logs/artists.txt")

        self.total_mts_files = 0
        self.total_converted_files = 0
        self.total_failed_conversions = 0
        self.total_failed_probes = 0
        self.total_missing_files = 0
        self.total_conversion_time = 0
        self.total_conversion_time_str = ""

    def fetch_artists(self, file) ->dict[int, str]:
        # Check if the file exists
        if not os.path.exists(file):
            return {}

        # Read artists from a file with the following format : id, name
        with open(file, "r") as f:
            data = f.read().split("\n")
            artists = {}
            for line in data:
                if line == "":
                    continue
                id, name = line.split(",")
                artists[name] = int(id)

        # Get max id for the artists
        max_id = max(artists.values())
        return artists

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

        self.print_summary()

    def print_summary(self):
        print(f"Total MTS files: {self.total_mts_files}")
        print(f"Total converted files: {self.total_converted_files}")
        print(f"Total failed conversions: {self.total_failed_conversions}")
        print(f"Total failed probes: {self.total_failed_probes}")
        print(f"Total missing files: {self.total_missing_files}")
        print(f"Total conversion time: {self.total_conversion_time_str}")

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
        conversion_log = []
        failed_conversions = []
        failed_probes = []

        dbf = dbf[:-4]

        mts_files = [f for f in os.listdir(self.audio_folder+"/"+dbf+"/") if f.endswith('.MTS')]

        total_conversion_time = 0.0
        total_converted_files = 0
        for i, record in enumerate(data):
            # Get .MTS file count in the audio folder
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

            # Get size in KB of input_file
            size = os.path.getsize(input_file) / 1024

            conversion_msg = f"{i+1}.Converting: {input_file} ({size:.2f} KB) => {output_file}"
            print(conversion_msg, end="\r")
            
            audio_converted = False

            start_time = timer()

            try:
                os.system(f"ffmpeg -y -i {input_file} -nostats -loglevel 0 -c:a libvorbis -q:a 4 -vsync 2 {output_file}")
                audio_converted = True
            except:
                failed_conversions.append(record)
                continue

            end_time = timer()
            time_diff = timedelta(seconds=end_time - start_time)
            print(f"{conversion_msg}... Done. Time: {time_diff}")

            total_conversion_time += time_diff.total_seconds()
            total_converted_files += 1

            record["conversion_time"] = time_diff.total_seconds()

            duration = 0
            if audio_converted:
                try:
                    duration = self.probe_audio_duration(output_file)
                except:
                    failed_probes.append(record)

            record["duration"] = duration * 1000 # milliseconds
            converted_files.append(record)

            # check if the record 'artist' is in the artists dictionary if not add it
            if record['artist'] not in self.artists:
                self.artists[record['artist']] = len(self.artists)

        print(f"Total Files Converted: {total_converted_files}")

        # Print total conversion time as "hh:mm:ss" format
        total_time = timedelta(seconds=total_conversion_time)
        print(f"Total conversion time: {total_time}")

        # Get average conversion time in seconds
        average_conversion_time = total_conversion_time / total_converted_files

        # Print average conversion time as "hh:mm:ss" format
        average_conversion_time = timedelta(seconds=average_conversion_time)
        print(f"Average conversion time: {average_conversion_time}")

        dt0 = datetime.datetime(1,1,1)

        log = {"category":dbf,
                "dbf_file":dbf+".DBF",
                "dbf_records":len(data),
                "audio_files":len(mts_files),
                "converted_files_count":len(converted_files),
                "converted_files":converted_files,
                "total_conversion_time":(dt0+total_time).strftime('%H:%M:%S'), 
                "average_conversion_time":(dt0+average_conversion_time).strftime('%H:%M:%S'),
                "failed_conversions":failed_conversions,
                "failed_probes":failed_probes,
                "missing_files":missing_files
                }

        self.total_mts_files += len(mts_files)
        self.total_converted_files += len(converted_files)
        self.total_failed_conversions += len(failed_conversions)
        self.total_failed_probes += len(failed_probes)
        self.total_missing_files += len(missing_files)
        self.total_conversion_time += total_time.total_seconds()
        self.total_conversion_time_str = (dt0+timedelta(seconds=self.total_conversion_time)).strftime('%H:%M:%S')

        conversion_log.append(log)
        
        # Write converted files to a json file
        # Remove extension .DBF
        with open(f"{self.log_folder}/{dbf}_converted.json", "w") as f:
            json.dump(converted_files, f, indent=4) 
        
        # Write missing files to a json file
        # Remove extension .DBF
        with open(f"{self.log_folder}/{dbf}_missing.json", "w") as f:
            json.dump(missing_files, f, indent=4)

        # Write conversion log to a json file
        with open(f"{self.log_folder}/{dbf}_conversion_log.json", "w") as f:
            json.dump(conversion_log, f, indent=4)

        # Write artists to the artists text file, if the artist file does not exists, create it
        with open(f"{self.log_folder}/artists.txt", "w") as f:
            for artist, id in self.artists.items():
                f.write(f"{id},{artist}\n")

    def probe_audio_duration(self, audio_file: str)-> float:
        # Get audio duration in seconds
        result = run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", audio_file], stdout=PIPE, stderr=PIPE)
        return float(result.stdout.decode("utf-8"))
