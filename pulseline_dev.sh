
#!/bin/bash

show_help() {
    cat << EOF
===========================================
 Script: pulseline.sh
===========================================

Usage: $(basename "$0") [OPTIONS]

Options:
    -i INPUT,       Specify the input observation file (default: None).
    -g GPU_FLAG,    Specify the GPU configuration (default: 0).
                    0 → Commensal configuration (GPUs: 40,41,42,43,44)
                    1 → Maintenance slot configuration (GPUs: 00,01,02,03,04,05,08–17, 40–44)
                    2 → Custom configuration (provide GPU IDs explicitly)

Examples:
    ./pulseline.sh -i inp_obs_dirs.txt
    ./pulseline.sh -i inp_obs_dirs.txt -g 0
    ./pulseline.sh -i inp_obs_dirs.txt -g 1
    ./pulseline.sh -i inp_obs_dirs.txt -g 2
    ./pulseline.sh -i inp_obs_dirs.txt -g 2 00 01 02 40 41 42

Description:
  - Populates obs_details.txt with scan details from BeamData.
  - Extracts target name, total beams, and uGMRT band from .ahdr files.
  - Sets Input_file=1 if FilData exists, otherwise 0.
  - Configures GPU resources according to the GPU_FLAG.
  - Finally runs 'python3 pulsar_search.py' in the pipeline directory.
===========================================
EOF
}

def_colors() {
    # Bright foreground colors
    BRED='\e[91m'
    BGRN='\e[92m'
    BYLW='\e[93m'
    BBLU='\e[94m'
    BMAG='\e[95m'
    BCYN='\e[96m'
    BWHT='\e[97m'
    DGRN='\e[38;5;22m'   # Dark Green

    # Bold
    BLD='\e[1m'

    # Reset formatting
    RST='\e[0m'
}

print_art() {
    echo -e ""
    echo -e "${BLD}${BRED}          ███████╗ ███████╗ ████████╗ ██████╗  ██╗ ██████╗  ███████╗          ${RST}"
    echo -e "${BLD}${BRED}          ██╔════╝ ██╔════╝ ╚══██╔══╝ ██╔══██╗ ██║ ██╔══██╗ ██╔════╝          ${RST}"
    echo -e "${BLD}${BRED}          █████╗   █████╗      ██║    ██████╔╝ ██║ ██████╔╝ █████╗            ${RST}"
    echo -e "${BLD}${BRED}          ██╔══╝   ██╔══╝      ██║    ██╔═══╝  ██║ ██╔═══╝  ██╔══╝            ${RST}"
    echo -e "${BLD}${BRED}          ██║      ██║         ██║    ██║      ██║ ██║      ███████╗          ${RST}"
    echo -e "${BLD}${BRED}          ╚═╝      ╚═╝         ╚═╝    ╚═╝      ╚═╝ ╚═╝      ╚══════╝          ${RST}"
    echo -e "${BLD}${BWHT}------------------------------------------------------------------------------${RST}"
    echo -e "${BLD}${BWHT}------------------------ SPOTLIGHT FFT PIPELINE v1.0 -------------------------${RST}"
    echo -e "${BLD}${BWHT}------------------------------------------------------------------------------${RST}"
    echo -e ""
    echo -e "${BLD}${DGRN}                     Copyright © 2025 The SPOTLIGHT Team                      ${RST}"
    echo -e "${BLD}${DGRN}               Code at https://github.com/nsmspotlight/fftpipe                ${RST}"
    echo -e "${BLD}${DGRN}     Report any issues at https://github.com/nsmspotlight/fftpipe/issues      ${RST}"
    echo -e ""
    echo -e ""
}

sanity_checks() {
    # Check if the input observation file is provided
    if [[ -z "$input_obs_file" ]]; then
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Input observation file is not provided. Use -i to specify it.${RST}"
        exit 1
    fi

    NOBS=$(sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$/d' "$input_obs_file" | grep -c "")
    if [[ -z "$NOBS" || "$NOBS" -eq 0 ]]; then
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Either the input observation file is empty or it does not have any uncommented lines.${RST}"
        exit 1
    else
        echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Number of observation directories to process: $NOBS."
    fi
}

gen_node_list_file() {
    nodes_list=()
    case $GPU_FLAG in
        0)
            nodes_list=(rggpu40 rggpu41 rggpu42 rggpu43 rggpu44)
            ;;
        1)
            nodes_list=(rggpu00 rggpu01 rggpu02 rggpu03 rggpu04 rggpu05 rggpu08 rggpu09 rggpu10 rggpu11 rggpu12 rggpu13 rggpu14 rggpu15 rggpu16 rggpu17 rggpu40 rggpu41 rggpu42 rggpu43 rggpu44)
            ;;
        2)
            shift $((OPTIND -1))
            if [[ $# -eq 0 ]]; then
                echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Reading the GPU node list from $GPU_FILE in the manual mode."
                echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Please ensure that the list of GPU nodes in $GPU_FILE is correct."
                if [[ ! -f "$GPU_FILE" ]]; then
                    echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # GPU file '$GPU_FILE' does not exist.${RST}"
                    exit 1
                fi
                mapfile -t nodes_list < <(grep -v '^[[:space:]]*#' "$GPU_FILE" | grep -v '^[[:space:]]*$')
                if [[ ${#nodes_list[@]} -eq 0 ]]; then
                    echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # GPU file '$GPU_FILE' is empty.${RST}"
                    exit 1
                fi
            else
                for G in "$@"; do
                    if [[ ! "$G" =~ ^-?[0-9]+$ ]] || (( G < 0 || G > 44 )); then
                        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Invalid GPU ID '$G'. Must be an integer between 00 and 44.${RST}"
                        exit 1
                    fi
                    nodes_list+=("rggpu$G")
                done
            fi
            ;;
        *)
            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Invalid GPU_FLAG value. Use 0, 1, or 2.${RST}"
            exit 1
            ;;
    esac

    echo -n "" > $GPU_FILE
    for G in "${nodes_list[@]}"; do
        echo "$G" >> $GPU_FILE
    done
}

xtract_N_chk() {
    RAW_FILES=$(eval echo "$BEAM_DIR/$SCAN.raw.{0..$UPPER}")
    echo "${nodes_list[0]}"
    remote_output=$(ssh -t -t "${nodes_list[0]}" "
        source ${TDSOFT}/env.sh;
        xtract2fil \
            ${RAW_FILES} \
            --output ${OBS_DIR} \
            --scan ${SCAN} \
            --dual \
            --tbin ${tbin} \
            --fbin ${fbin} \
            --nbeams ${NBEAMS} \
            --njobs ${njobs} \
            --offset ${offset};
        echo \$?")

    local_ssh_status=$?
    if [ "$local_ssh_status" -eq 255 ]; then
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # SSH connection failed.${RST}"
        echo -e "${BLD}${BMAG}$(date '+%Y-%m-%d %H:%M:%S') # HELP # SSH command exit status: $local_ssh_status${RST}"
        exit 1
    fi
    exit_code=$(echo "$remote_output" | tail -n 1 | tr -d '\r')
    if [ "$exit_code" -eq 0 ]; then
        echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Successfully extracted the beams into filterbank files for scan: $SCAN"
        cp "$BEAM_DIR/$SCAN.raw.*.ahdr" "$output_dir/state/$SCAN"
        # rm -f "$BEAM_DIR/$SCAN.raw.*"
        return 0
    else
        echo "$remote_output"
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # xtract2fil command failed for scan: $SCAN with exit code $exit_code.${RST}"
        return 1
    fi
}

gen_conf_file() {
    OBS_NAME=$(basename "$OBS_DIR")

    # Extract frequency (integer Hz) from AHDR
    FREQ=$(grep -m1 "Frequency Ch. 0" "$AHDR" \
           | awk -F= '{print $2}' \
           | awk '{print $1}' \
           | cut -d. -f1)

    echo "FREQ (Hz) = <$FREQ>"

    # Decide GMRT band
    if (( FREQ <= 500000000 )); then
        BAND="BAND3"
    elif (( FREQ >= 550000000 && FREQ <= 950000000 )); then
        BAND="BAND4"
    elif (( FREQ >= 1000000000 && FREQ <= 1460000000 )); then
        BAND="BAND5"
    else
        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Unknown frequency $FREQ Hz in $AHDR.${RST}"
        BAND="UNKNOWN"
    fi

    # Write to obs file
    {
        echo "# GTAC_scan                                 Target                  Total_beams        uGMRT_band          Input_file (0 for Raw, 1 for FIL)"
        echo "$OBS_NAME           $SCAN            $TOTAL_BMS                $BAND               $INP_FILE_FLAG"
    } > "$OBS_FILE"
}

analysis() {
    # Read non-comment, non-empty lines into an array
    mapfile -t obs_dirs < <(grep -v '^[[:space:]]*#' "$input_obs_file" | grep -v '^[[:space:]]*$')
    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Observation directories to be processed:"
    for OBS_DIR in "${obs_dirs[@]}"; do
        echo "                                                                  - $(basename "$OBS_DIR")"
    done

    # Final checks and launch the FFT processing script
    for OBS_DIR in "${obs_dirs[@]}"; do
        if [[ ! -d "$OBS_DIR" ]]; then
            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Observation directory '$OBS_DIR' does not exist or is not a directory.${RST}"
            continue # Skip to the next line if the directory is invalid
        fi

        echo -e "${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Starting to process the following observation: $(basename $OBS_DIR)${RST}"

        BEAM_DIR="${OBS_DIR}/BeamData"
        FIL_DIR="${OBS_DIR}/FilData"

        if [[ ! -d "$BEAM_DIR" && ! -d "$FIL_DIR" ]]; then
            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Neither Beam data directory '$BEAM_DIR' nor Fil data directory '$FIL_DIR' exist.${RST}"
            continue # Skip this observation
        
        elif [[ ! -d "$BEAM_DIR" && -d "$FIL_DIR" ]]; then
            # Beam data missing, but Fil data exists
            for SCAN_DIR in "$FIL_DIR"/*; do
                [[ -d "$SCAN_DIR" ]] || continue  # skip non-directories
                SCAN=$(basename "$SCAN_DIR")      # Scan name is the directory name
                AHDR="$SCAN_DIR/$SCAN.raw.0.ahdr" # The .ahdr file inside that directory

                if [[ ! -f "$AHDR" ]]; then
                    echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # .ahdr file not found in $SCAN_DIR${RST}"
                    continue
                fi

                #NBEAMS=$(grep -m1 "Total No. of Beams/host[[:space:]]*=" "$AHDR" | awk -F= '{print $2}' | xargs)
                NBEAMS=10
                echo -e "Number of beams in FilData are: $NBEAMS"
                if [[ $NBEAMS == 0 ]]; then
                    echo -e "There is some issue with the .adhr file"
                    continue
                fi
                #TOTAL_BMS=$(grep -m1 "Total No. of Beams[[:space:]]*=" "$AHDR" | awk -F= '{print $2}' | xargs)
                TOTAL_BMS=160
                NHOSTS=$(( TOTAL_BMS / NBEAMS ))
                UPPER=$((NHOSTS-1))

                echo -e "Scans currently processing (FIL_DIR mode): $SCAN"

                # Here we assume .fil files are already extracted inside $SCAN_DIR
                if (( $(ls "$SCAN_DIR"/*.fil 2>/dev/null | wc -l) == TOTAL_BMS )); then
                    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Found the expected number of filterbank files in $SCAN_DIR."
                    INP_FILE_FLAG=1
                else
                    echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Expected $TOTAL_BMS filterbank files in $SCAN_DIR but found different.${RST}"
                    continue
                fi

                gen_conf_file
                cd "$PIPELINE_DIR" || exit
                python3 pulsar_search.py
            done

        else
            for SCAN in "$BEAM_DIR"/*.raw.0.ahdr; do
                AHDR="$SCAN"
                #NBEAMS=$(grep -m1 "Total No. of Beams/host[[:space:]]*=" "$AHDR" | awk -F= '{print $2}' | xargs)
                NBEAMS=10

                echo -e "Number of beams in BeamData are: $NBEAMS"
                if (( NBEAMS == 0 )); then
                    echo -e "There is some issue with the .adhr file"
                    continue
                fi

                #TOTAL_BMS=$(grep -m1 "Total No. of Beams[[:space:]]*=" "$AHDR" | awk -F= '{print $2}' | xargs)
                TOTAL_BMS=160
                if (( NBEAMS > 0 )); then
                    NHOSTS=$(( TOTAL_BMS / NBEAMS ))
                else
                    echo "[ERROR] NBEAMS is 0, cannot divide. Skipping"
                    continue
                fi
                UPPER=$((NHOSTS-1))

                SCAN=$(basename -s .raw.0.ahdr "$SCAN")

                echo -e "Scans currently processing is: $SCAN"

                if [[ -d "$FIL_DIR/$SCAN" ]] && (( $(ls "$FIL_DIR/$SCAN"/*.fil 2>/dev/null | wc -l) == TOTAL_BMS )); then
                    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Found the expected number of filterbank files for the scan: $SCAN."
                    INP_FILE_FLAG=1
                else
                    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Couldn't find the expected number of filterbank files for the scan: $SCAN."
                    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Attempting to extract beams using xtract2fil."
                    if [[ ! -f "$BEAM_DIR/$SCAN.raw.0" ]]; then
                        echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Raw file '$BEAM_DIR/$SCAN.raw.0' does not exist. Cannot run xtract2fil.${RST}"
                        continue
                    else
                        INP_FILE_FLAG=0
                        xtract_N_chk
                        if [[ $? -eq 1 ]]; then
                            exit 1
                        fi
                        INP_FILE_FLAG=1
                    fi
                fi

                gen_conf_file

                echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Launching the FFT pipeline."
                cd "$PIPELINE_DIR" || exit
                python3 pulsar_search.py

            done
        fi

        echo -e "${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Done processing the following observation: $(basename $OBS_DIR)${RST}"
    done
}

main() {
    input_obs_file=""   # Stores the value for the -i option
    GPU_FLAG=0          # Default GPU flag
    tbin=10
    fbin=4
    offset=0
    njobs=16

    # Parse command line options
    if [[ $# -eq 0 ]]; then
        show_help
        exit 0
    fi
    while getopts "g:i:h" OPT; do
        case $OPT in
        i)
            # Check if a value is provided for -i
            if [[ -z "$OPTARG" || "$OPTARG" == -* ]]; then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # -i option requires a value.${RST}"
                exit 1
            else
                if [[ ! -f "$OPTARG" ]]; then
                    echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Input observation file '$OPTARG' does not exist.${RST}"
                    exit 1
                else
                    input_obs_file="$OPTARG"
                    echo -e "${BCYN}$(date '+%Y-%m-%d %H:%M:%S') # INFO #${RST} Input observation file is set to: $input_obs_file"
                fi
            fi
            ;;
        g)
            if [[ "$OPTARG" =~ ^-?[0-9]+$ ]] && (( OPTARG < 0 || OPTARG > 2 )); then
                echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # -g option requires a value of 0, 1, or 2.${RST}"
                exit 1
            fi
            GPU_FLAG="$OPTARG"
            ;;
        h)
            show_help
            exit 0
            ;;
        \?)
            echo -e "${BLD}${BRED}$(date '+%Y-%m-%d %H:%M:%S') # ERROR # Unknown option: $OPTARG${RST}"
            show_help
            exit 1
            ;;
        *)
            show_help
            exit 1
            ;;
        esac
    done
    
    gen_node_list_file "$@"

    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Performing sanity checks...${RST}"
    sanity_checks
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Sanity checks passed.${RST}"
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # LOG # Starting analysis...${RST}"
    echo "FFTPipe status = ON" > "$STATUS_LOG_FILE"
    echo "Nodes = ${nodes_list[@]}" >> "$STATUS_LOG_FILE"
    analysis
    echo -e "${BLD}${BGRN}$(date '+%Y-%m-%d %H:%M:%S') # SUCCESS # Analysis completed.${RST}"
    echo "FFTPipe status = OFF" > "$STATUS_LOG_FILE"
}

def_colors

print_art

# Load environment
source /lustre_archive/apps/tdsoft/env.sh

PIPELINE_DIR="/lustre_data/spotlight/data/pulsar_search_pipeline_dev"
OBS_FILE="${PIPELINE_DIR}/GTAC_obs_details/obs_details.txt"
GPU_FILE="${PIPELINE_DIR}/GPU_resources/avail_gpu_nodes.txt"
STATUS_LOG_FILE="/lustre_archive/spotlight/data/MON_DATA/das_log/FFTPipe_status.log"

main "$@"
