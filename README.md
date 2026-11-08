# SPHERE
### SPOTLIGHT High-performance Engine for Radio pulsar Exploration

**SPHERE** is a GPU-accelerated pulsar-search pipeline developed to process beamformed data recorded by the **SPOTLIGHT commensal backend** of the upgraded Giant Metrewave Radio Telescope (uGMRT).

The pipeline performs large-scale offline periodicity searches across multiple beams, combining radio-frequency interference (RFI) mitigation, GPU-accelerated dedispersion and FFT-based periodicity searches, candidate sifting, folding, and machine-learning-based candidate classification.

SPOTLIGHT is primarily designed for real-time detection and localisation of Fast Radio Bursts (FRBs). It also records selected beamformed data products, enabling pulsar searches commensally without requiring dedicated telescope observing time.

---

## Key Features

- **Multi-GPU processing:** Utilises the AstroAccelerate framework for GPU-accelerated pulsar searches.
- **RFI mitigation:** Incorporates RFI mitigation using the PulsarX FILTOOL.
- **Dedispersion and periodicity searches:** Performs dedispersion and FFT-based periodicity searches over the selected DM and period parameter space.
- **Harmonic summing:** Searches for weak periodic signals using harmonic summing.
- **Multi-beam candidate sifting:** Processes candidates independently across multiple beams and applies spatial and statistical selection techniques.
- **Candidate folding:** Uses PulsarX PSRFOLD to generate diagnostic plots for candidate evaluation.
- **Machine-learning classification:** Integrates the Pulsar Image-based Classification System (PICS) for automated candidate classification.

---

## Pipeline Overview

SPHERE consists of the following major processing stages:

### 1. Data Preparation and RFI Mitigation

The input beamformed filterbank data are prepared for periodicity searches. RFI mitigation is performed using the FILTOOL utility available in PulsarX to reduce the impact of radio-frequency interference on subsequent processing.

### 2. GPU-accelerated FFT Periodicity Search

The RFI-mitigated data are processed using AstroAccelerate.

The search stage includes:

- Dedispersion over the specified dispersion measure (DM) range.
- FFT-based periodicity searches.
- Harmonic summing to enhance the detectability of periodic signals.

### 3. Candidate Sifting and Optimisation

The candidate-selection procedure incorporates:

Independent ebam sifting
- Independent candidate detection and sifting across the recorded beams.

Spatial clustering
- Spatial clustering of detections across beams.
- Selection of representative detections from spatial clusters.
- Ranking of isolated detections using harmonic significance and signal-to-noise ratio (S/N).
- Candidate reduction while retaining potentially interesting pulsar signals.

### 4. Candidate Folding

The selected candidates are folded using PulsarX PSRFOLD. Folding produces diagnostic plots that allow the periodic nature and signal characteristics of each candidate to be examined.

### 5. Candidate Classification

The folded candidates are passed through PICS, a machine-learning-based pulsar candidate classification framework. The classifier assigns candidate probabilities to help distinguish likely pulsar signals from non-pulsar detections.

---

## Pipeline Workflow

```text
       SPOTLIGHT Beamformed Data
                  |
                  v
         RFI Mitigation
       (PulsarX FILTOOL)
                  |
                  v
       GPU-accelerated Search
          (AstroAccelerate)
                  |
          +-------+-------+
          |               |
          v               v
      Dedispersion     FFT Search
          |               |
          +-------+-------+
                  |
                  v
          Harmonic Summing
                  |
                  v
       Multi-beam Candidate
             Sifting
                  |
                  v
        Candidate Selection
                  |
                  v
         Candidate Folding
       (PulsarX PSRFOLD)
                  |
                  v
       Candidate Classification
                (PICS)
                  |
          +-------+-------+
          |               |
          v               v
   Likely Pulsars    Non-pulsar
     Candidates      Candidates
```

---

## Software and Dependencies

SPHERE relies on the following software packages and frameworks:

| Software | Purpose |
|---|---|
| Python | Pipeline orchestration and data processing |
| AstroAccelerate | GPU-accelerated dedispersion and FFT periodicity searches |
| PulsarX | RFI mitigation and candidate folding |
| PICS | Machine-learning-based candidate classification |
| NumPy | Numerical operations and data handling |
| CUDA and NVIDIA GPU libraries | GPU execution, as required by the installed AstroAccelerate environment |

The exact software versions and build configurations depend on the computing environment in which SPHERE is deployed.

Refer to the respective software repositories for installation and configuration instructions.

---

## Hardware and Computing Environment

SPHERE was developed for large-scale processing of beamformed data from the SPOTLIGHT backend.

The pipeline is designed to utilise multi-GPU computing resources to process the large volume of data generated by the commensal observations.

The computational requirements depend on the number of beams, observation duration, sampling parameters, DM search range, and the selected search configuration.

---

## Scientific Validation

The pipeline was evaluated using known pulsar signals present in uGMRT observations.

Its performance was validated through the successful redetection of known pulsars in SPOTLIGHT commissioning and observing data. The processing framework was designed to handle the recorded multi-beam data volume within the available offline computing resources.

Population-synthesis modelling, together with the available SPOTLIGHT sky coverage, was also used to investigate the potential pulsar discovery yield of the survey.

For further details about the pipeline design, search strategy, candidate-selection procedure, and scientific performance, please refer to the associated publication.

---

## Repository

**GitHub:** https://github.com/jyotirmoydas5392/spotlight_pulsar_search_pipeline

The repository contains the implementation of the SPHERE pipeline and its associated scripts.

Please refer to the repository files and configuration templates for implementation-specific instructions.

---

## Publication and Citation

If you use SPHERE or its associated search strategy in your research, please cite the following work:

**Das, J., et al. (2026).** *The SPOTLIGHT Pulsar Search Pipeline: A GPU-Accelerated FFT Approach.*

[arXiv:2607.08854](https://arxiv.org/abs/2607.08854)

```bibtex
@misc{das2026spotlightpulsarsearchpipeline,
  title={The SPOTLIGHT Pulsar Search Pipeline:
         A GPU-Accelerated FFT Approach},
  author={Das, Jyotirmoy and others},
  year={2026},
  eprint={2607.08854},
  archivePrefix={arXiv},
  primaryClass={astro-ph.HE},
  url={https://arxiv.org/abs/2607.08854}
}
```

---

## Acknowledgements

SPHERE makes use of the following open-source software and frameworks:

- [AstroAccelerate](https://github.com/AstroAccelerateOrg/astro-accelerate)
- [PulsarX](https://github.com/ypmen/PulsarX)
- [PICS / UBC AI](https://github.com/zhuww/ubc_AI)

We acknowledge the developers and contributors of these software packages for making their tools available to the pulsar astronomy community.

---

## Contact

Jyotirmoy Das
National Centre for Radio Astrophysics (NCRA)
Tata Institute of Fundamental Research (TIFR), Pune, India

Email: tataidas5392@gmail.com

GitHub: jyotirmoydas5392

For questions, suggestions, bug reports, or collaboration related to SPHERE, feel free to contact me by email or open an issue in this repository.
