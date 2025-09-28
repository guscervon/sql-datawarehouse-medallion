import pandas as pd
import glob
import io

PATH_CSV = "./datasets"


def main():
    csv_files = glob.glob(f"{PATH_CSV}/**/*.csv", recursive=True)
    print(csv_files)

    with open("datasets_info.txt", "w", encoding="utf-8") as f:
        for file in csv_files:
            df = pd.read_csv(file)

            buffer = io.StringIO()
            df.info(buf=buffer)
            info_str = buffer.getvalue()

            f.write(f"--- File: {file} ---\n")
            f.write(info_str)
            f.write("\n\n")


if __name__ == "__main__":
    main()
