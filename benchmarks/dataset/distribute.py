import os
import argparse

def rawcount(filename):
    lines = 0

    with open(filename, 'rb') as f:
        buf_size = 1024 * 1024
        read_f = f.raw.read

        buf = read_f(buf_size)
        while buf:
            lines += buf.count(b'\n')
            buf = read_f(buf_size)

    return lines


def main():
    parser = argparse.ArgumentParser(description='Distribute NDJSON lines across multiple files')
    parser.add_argument('directory', type=str, help='Path to directory containing .log files')
    parser.add_argument('--num-files', '-n', type=int, required=True, help='Number of output files')
    args = parser.parse_args()

    if not os.path.isdir(args.directory):
        print(f"Error: Directory '{args.directory}' does not exist")
        return

    if args.num_files < 1:
        print("Error: Number of files must be at least 1")
        return

    input = []
    input_count = 0

    for filename in os.listdir(args.directory):
        if filename.endswith('.log'):
            full_path = os.path.join(args.directory, filename)
            input_count += rawcount(full_path)
            input.append(open(full_path, 'r'))

    print(f"Total lines found: {input_count}")

    if input_count == 0:
        print("No lines to distribute")
        return

    base_chunk_size = input_count // args.num_files

    index = 0
    for i in range(args.num_files):
        written = 0

        output = open(f"docs-{i}.log", "w+")
        file = input[index]

        while written < base_chunk_size:
            line = file.readline()

            if line == "":
                file.close()

                index += 1
                file = input[index]

                continue

            output.write(line)
            written += 1

        output.close()

if __name__ == "__main__":
    main()

