import os

# ==== CONFIGURATION ====
start_year = 2000
end_year = 2023

folder_path = r"C:\Users\3D\OneDrive\Documents\SQL Server Management Studio 21\IPEDSDataWarehouse\Silver\ipeds_datawarehouse\models\sources\hd"
file_prefix = "hd"
file_suffix = ".sql"

# Optional: Template content for inside each file
template = ""

# ==== SCRIPT ====
os.makedirs(folder_path, exist_ok=True)

for year in range(start_year, end_year + 1):
    filename = f"{file_prefix}{year}{file_suffix}"
    file_path = os.path.join(folder_path, filename)

    with open(file_path, "w") as f:
        f.write(template.format(year=year, prefix=file_prefix))

    print(f"Created: {filename}")

print("\nDone!")

