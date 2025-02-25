import pandas as pd
import numpy as np
import datetime
import os

def to_silver():
    INPUT_DIR = '/mnt/c/Users/Caio Mesquita/Desktop/Data sc/Des_5/RID-156732_Desafio08/camadas/Bronze'
    OUTPUT_DIR = '/mnt/c/Users/Caio Mesquita/Desktop/Data sc/Des_5/RID-156732_Desafio08/camadas/Silver'  
    
    for file in os.listdir(INPUT_DIR):
        if file.endswith(".csv"):
            file_path = os.path.join(INPUT_DIR, file)
            data = pd.read_csv(file_path)
            data.dropna(subset=['name','email','date_of_birth'],inplace=True)
            data["email"] = data["email"].apply(lambda email: email.replace("example", "@example") 
                                                      if "@" not in email and "example" in email else email)
            data['date_of_birth'] = pd.to_datetime(data['date_of_birth'], errors='coerce')
            today = datetime.date.today()
            data['age'] = data['date_of_birth'].apply(lambda dob: today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day)))
            output_file_path = os.path.join(OUTPUT_DIR, file)
            data.to_csv(output_file_path,index=False)