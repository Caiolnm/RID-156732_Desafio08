import pandas as pd
import os

def upload_bronze():
    INPUT_DIR = '/mnt/c/Users/Caio Mesquita/Desktop/Data sc/Des_5/RID-156732_Desafio08/Dados'
    OUTPUT_DIR = '/mnt/c/Users/Caio Mesquita/Desktop/Data sc/Des_5/RID-156732_Desafio08/camadas/Bronze'    

    for file in os.listdir(INPUT_DIR):
        if file.endswith(".csv"):
            file_path = os.path.join(INPUT_DIR, file)
            df = pd.read_csv(file_path)
            output_file_path = os.path.join(OUTPUT_DIR, file)
            df.to_csv(output_file_path, index=False)