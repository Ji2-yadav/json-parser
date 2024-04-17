from tqdm import tqdm
import pandas as pd
import os

def save_file(data, idx, op_dir):
    filedata = '''{"provider_references" : [\n'''
    try:
        data = data.rsplit(',', 1)[0]
    except:
        pass
    filedata +=data
    filedata +='\n]\n}'
    file_path = f"{op_dir}/file{idx}.json"
    with open(file_path, "w") as f:
        f.write(filedata)

def main():
    input_file_path = "file1.json"
    op_dir = "./data"

    if not os.path.exists(op_dir):
        os.makedirs(op_dir)

    chunk_size = 1
    curr_lines_cnt = 0
    df = pd.read_json(input_file_path , lines=True, chunksize=chunk_size)
    file_size = os.path.getsize(input_file_path)
    curr_buffer_size = 0
    curr_line =''
    pr_flag = False
    max_saved_lines = 9000
    text_to_save = []
    file_idx = 1

    # print(file_size)
    pbar = tqdm(total=file_size)
    while(curr_buffer_size < file_size):
        curr_line = df.data.readline()
        len_ = len(curr_line.encode('utf-8'))
        curr_buffer_size += len_
        pbar.update(len_)
        if(not pr_flag):
            if(curr_line == '''"provider_references":[\n''' ):
                pr_flag = True
        else:
            if(curr_lines_cnt<=max_saved_lines):
                text_to_save.append(curr_line)
                curr_lines_cnt+=1
            else:
                text = "".join(text_to_save)
                save_file(text, file_idx, op_dir)
                curr_lines_cnt = 0
                text_to_save.clear()
                file_idx +=1

    text = "".join(text_to_save)
    save_file(text, file_idx, op_dir)            

if __name__ == "__main__":
    main()