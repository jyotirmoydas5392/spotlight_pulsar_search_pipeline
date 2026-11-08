import os, sys, glob, shutil, logging, argparse

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

base_dir = os.getenv("PULSELINE_VER1_DIR")
if not base_dir:
    logging.error("PULSELINE_VER1_DIR environment variable is not set.")
    sys.exit(1)

sys.path.insert(0, os.path.join(base_dir, "input_file_dir_init/scripts"))
from read_input_file_dir import load_parameters

config_file_path = os.path.join(base_dir,"input_file_dir_init/input_dir/input_file_directory.txt")
if not os.path.isfile(config_file_path):
    logging.error("Configuration file not found: %s", config_file_path)
    sys.exit(1)

params = load_parameters(config_file_path)
model_file = params.get("PICS_model_file")
if not model_file:
    logging.error("PICS_model_file not specified.")
    sys.exit(1)
model_name = os.path.splitext(model_file)[0]

def read_classifier_file(filepath):
    results={}
    if not os.path.isfile(filepath):
        return results
    with open(filepath,"r") as f:
        for line in f:
            cols=line.split()
            if len(cols)!=2:
                continue
            try:
                results[cols[0]]=float(cols[1])
            except ValueError:
                pass
    return results

def generate_final_results(output_dir):
    result_files=sorted(glob.glob(os.path.join(output_dir,f"{model_name}_*.txt")))
    if not result_files:
        logging.error("No classifier outputs found for %s",model_name)
        return None
    combined={}
    for fn in result_files:
        logging.info("Reading %s",os.path.basename(fn))
        for ar,prob in read_classifier_file(fn).items():
            combined[ar]=max(combined.get(ar,prob),prob)
    if not combined:
        return None
    final_file=os.path.join(output_dir,f"final_{model_name}.txt")
    with open(final_file,"w") as f:
        for ar in sorted(combined):
            f.write(f"{ar} {combined[ar]:.6f}\n")
    return final_file

def split_and_copy_candidates(final_file,ar_dir,output_dir,threshold):
    pos_dir=os.path.join(output_dir,"positive_candidates")
    neg_dir=os.path.join(output_dir,"negative_candidates")
    os.makedirs(pos_dir,exist_ok=True)
    os.makedirs(neg_dir,exist_ok=True)
    with open(final_file) as fin:
        for line in fin:
            cols=line.split()
            if len(cols)!=2:
                continue
            ar_name=cols[0]
            try:
                prob=float(cols[1])
            except ValueError:
                continue
            base=os.path.splitext(ar_name)[0]
            target=pos_dir if prob>=threshold else neg_dir
            for src in glob.glob(os.path.join(ar_dir,base+".*")):
                dst=os.path.join(target,os.path.basename(src))
                if not os.path.exists(dst):
                    shutil.copy2(src,dst)

def generate_PICS_results(input_dir,output_dir,threshold):
    final_file=generate_final_results(output_dir)
    if final_file:
        split_and_copy_candidates(final_file,input_dir,output_dir,threshold)

def main():
    p=argparse.ArgumentParser(description="Merge node classifier outputs and split candidates.")
    p.add_argument("input_dir")
    p.add_argument("output_dir")
    p.add_argument("threshold",type=float)
    a=p.parse_args()
    os.makedirs(a.output_dir,exist_ok=True)
    generate_PICS_results(a.input_dir,a.output_dir,a.threshold)

if __name__=="__main__":
    main()