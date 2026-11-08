from scripts.load_parameters import load_parameters

# Load parameters from config file
params = load_parameters("input_files/Test_input.txt")

# Access parameters
fil_file = params['fil_file']
start_DM = params['start_DM']
end_DM = params['end_DM']
dm_step = params['dm_step']
workers = params['workers']

# Use these parameters in your script logic
print(f"Processing file: {fil_file} with DM range {start_DM} to {end_DM}")
