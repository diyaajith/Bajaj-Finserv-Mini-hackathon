import pandas as pd
import json
import hashlib
import re
from datetime import datetime

df = pd.read_json('DataEngineeringQ2.json')

df = df[['appointmentId', 'phoneNumber', 'patientDetails', 'consultationData']]

df['firstName'] = df['patientDetails'].apply(lambda x: x['firstName'])
df['lastName'] = df['patientDetails'].apply(lambda x: x['lastName'])
df['gender'] = df['patientDetails'].apply(lambda x: x.get('gender', 'others'))
df['DOB'] = df['patientDetails'].apply(lambda x: x.get('birthDate'))

df['fullName'] = df['firstName'] + ' ' + df['lastName']

df['medicines'] = df['consultationData'].apply(lambda x: x['medicines'])

def is_valid_mobile(number):
    pattern = r'^(?:\+91|91)?([6-9]\d{9})$'
    match = re.match(pattern, number)
    if match:
        return True
    else:
        return False

df['isValidMobile'] = df['phoneNumber'].apply(is_valid_mobile)

def generate_hash(number):
    return hashlib.sha256(number.encode()).hexdigest()

df['phoneNumberHash'] = df['phoneNumber'].apply(lambda x: generate_hash(x) if is_valid_mobile(x) else None)

def calculate_age(dob):
    if dob:
        dob_datetime = datetime.strptime(dob, '%Y-%m-%dT%H:%M:%S.%fZ')
        today = datetime.now()
        age = today.year - dob_datetime.year
        if today.month < dob_datetime.month or (today.month == dob_datetime.month and today.day < dob_datetime.day):
            age -= 1
        return age
    else:
        return None

df['Age'] = df['DOB'].apply(calculate_age)

df['noOfMedicines'] = df['medicines'].apply(len)

df['noOfActiveMedicines'] = df['medicines'].apply(lambda x: sum(medicine['isActive'] for medicine in x))

df['noOfInactiveMedicines'] = df['medicines'].apply(lambda x: sum(not medicine['isActive'] for medicine in x))

def get_active_medicine_names(medicines):
    active_medicine_names = [medicine['medicineName'] for medicine in medicines if medicine['isActive']]
    return ', '.join(active_medicine_names)

df['medicineNames'] = df['medicines'].apply(get_active_medicine_names)

df_aggregated = df.groupby('appointmentId').agg({
    'fullName': 'first',
    'phoneNumber': 'first',
    'isValidMobile': 'first',
    'phoneNumberHash': 'first',
    'gender': 'first',
    'DOB': 'first',
    'Age': 'first',
    'noOfMedicines': 'sum',
    'noOfActiveMedicines': 'sum',
    'noOfInactiveMedicines': 'sum',
    'medicineNames': 'first'
}).reset_index()

df_final = df_aggregated[['appointmentId', 'fullName', 'phoneNumber', 'isValidMobile', 'phoneNumberHash', 'gender', 'DOB', 'Age', 'noOfMedicines', 'noOfActiveMedicines', 'noOfInactiveMedicines', 'medicineNames']]

print(df_final)
df_final.to_csv('output.csv', sep='~', index=False)

age = int(df_aggregated['Age'].mean())
gender = {k: int(v) for k, v in df_aggregated['gender'].value_counts().to_dict().items()}
valid_phone_numbers = int(df_aggregated['isValidMobile'].sum())
appointments = int(len(df_aggregated))
medicines = int(df_aggregated['noOfMedicines'].sum())
active_medicines = int(df_aggregated['noOfActiveMedicines'].sum())

data = {
    'Age': age,
    'gender': gender,
    'validPhoneNumbers': valid_phone_numbers,
    'appointments': appointments,
    'medicines': medicines,
    'activeMedicines': active_medicines
}

with open('output.json', 'w') as file:
    json.dump(data, file)