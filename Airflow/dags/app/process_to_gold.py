import pandas as pd
import numpy as np
import datetime
import os

def age_to_range(age):
    if 0 <= age <= 10:
        return "0-10"
    elif 11 <= age <= 20:
        return "11-20"
    elif 21 <= age <= 30:
        return "21-30"
    elif 31 <= age <= 40:
        return "31-40"
    elif 41 <= age <= 50:
        return "41-50"
    elif 51 <= age <= 60:
        return "51-60"
    elif 61 <= age <= 70:
        return "61-70"
    elif 71 <= age <= 80:
        return "71-80"
    elif 81 <= age <= 90:
        return "81-90"
    else:
        return "91-100"
def to_gold():
    INPUT_DIR = '/mnt/c/Users/Caio Mesquita/Desktop/Data sc/Des_5/RID-156732_Desafio08/camadas/Silver'
    OUTPUT_DIR = '/mnt/c/Users/Caio Mesquita/Desktop/Data sc/Des_5/RID-156732_Desafio08/camadas/Gold'
    for file in os.listdir(INPUT_DIR):
        if file.endswith(".csv"):
            file_path = os.path.join(INPUT_DIR, file)
            df = pd.read_csv(file_path)
            df['age_range'] = df['age'].apply(age_to_range)
            gold_data = df.groupby(['age_range', 'subscription_status']).size().reset_index(name='user_count')
            output_file_path = os.path.join(OUTPUT_DIR, file)
            gold_data.to_csv(output_file_path,index=False)