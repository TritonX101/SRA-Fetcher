# SRA-Fetcher: A high-speed CLI tool for batch fetching SRA metadata based on pysradb.

## Installation
### 1. Get the Source Code
You can clone the repository using Git or download the source code directly from GitHub.

**Clone via Git:**
\`\`\`bash
git clone https://github.com/TritonX101/SRA-Fetcher.git
cd SRA-Fetcher
\`\`\`

**Or Download:**
Click the **Code** button on the GitHub repository page and select **Download ZIP**.

### 2. Set Up Environment
It is highly recommended to run this program in an isolated **Conda** environment to avoid dependency conflicts.

#### Option A: Automatic Setup (Recommended)
You can automatically create the environment using the provided \`environment.yml\` file:

\`\`\`bash
conda env create -f environment.yml
conda activate SRA-Fetcher
\`\`\`

#### Option B: Manual Setup
If you prefer to configure the environment manually, please ensure you have **Python 3.10** installed along with the following libraries:

* \`pandas\`
* \`requests\`
* \`typer\`
* \`rich\`
* \`pysradb\`

You can install these dependencies via pip:

\`\`\`bash
pip install pandas requests typer rich pysradb
\`\`\`

---

