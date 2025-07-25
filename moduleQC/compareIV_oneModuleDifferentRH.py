import os

from pathlib import Path
import glob
from typing import List, Dict, Tuple, Union
import logging
import pandas as pd
import numpy as np
import pickle
import matplotlib as mpl
import matplotlib.pyplot as plt
mpl.rcParams.update(mpl.rcParamsDefault)
font = {"size": 20}
mpl.rc("font", **font)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IVCurveAnalyzer:
    """A class to analyze and visualize IV curve data from multiple modules and file formats."""
    
    COLORS = [
        'purple', 'orange', 'green', 'gray', 'cyan', 'brown', 'blue', 'red',
        'olive', 'pink', 'black', 'magenta', 'yellow', 'teal', 'navy', 'maroon'
    ]
    
    def __init__(self, data_dirs: List[Union[str, Path]], conditions: Dict[str, List[str]]):
        """
        Initialize the IVCurveAnalyzer with data directories and module-specific conditions.
        
        Args:
            data_dirs: List of directories containing input files (.txt or .pickle)
            conditions: Dictionary mapping module names to lists of condition labels
        """
        self.data_dirs = [Path(dir) for dir in data_dirs]
        self.conditions = conditions
        self.dataframes: Dict[str, pd.DataFrame] = {}  # Store DataFrames by module
        
    
    def _get_files(self, data_dir: Path) -> List[Path]:
        """Retrieve sorted list of .txt or .pickle files from the data directory."""
        try:
            return sorted([f for f in data_dir.glob('*') if f.suffix in ('.txt', '.pickle')])
        except Exception as e:
            logger.error(f"Error accessing directory {data_dir}: {e}")
            raise
    
    def _load_txt_file(self, file_path: Path, condition: str) -> pd.DataFrame:
        """Load data from a .txt file and prepare a DataFrame."""
        try:
            logger.info(f"Processing text file: {file_path.name}")
            df = pd.read_csv(
                file_path,
                sep=r'\s+',
                header=None,
                names=['Bias voltage', 'Leakage current']
            )
            df['Conditions'] = condition
            logger.debug(f"Text file DataFrame shape for {condition}: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Error processing text file {file_path}: {e}")
            raise
    
    def _load_pickle_file(self, file_path: Path, condition: str) -> pd.DataFrame:
        """Load data from a .pickle file and prepare a DataFrame."""
        try:
            logger.info(f"Processing pickle file: {file_path.name}")
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
            # Assume pickle contains a DataFrame or dict with 'Bias voltage' and 'Leakage current'
            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, dict):
                df = pd.DataFrame(data)
            else:
                raise ValueError(f"Unsupported pickle content in {file_path}")
            # Ensure required columns exist
            if not {'Bias voltage', 'Leakage current'}.issubset(df.columns):
                raise ValueError(f"Pickle file {file_path} missing required columns")
            df['Conditions'] = condition
            logger.debug(f"Pickle file DataFrame shape for {condition}: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Error processing pickle file {file_path}: {e}")
            raise
    
    def _load_file(self, file_path: Path, condition: str) -> pd.DataFrame:
        """Load data from a file based on its extension."""
        if file_path.suffix == '.txt':
            return self._load_txt_file(file_path, condition)
        elif file_path.suffix == '.pickle':
            return self._load_pickle_file(file_path, condition)
        else:
            raise ValueError(f"Unsupported file extension: {file_path.suffix}")
    
    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform DataFrame by taking absolute values and scaling leakage current."""
        df['Bias voltage'] = df['Bias voltage'].abs()
        df['Leakage current'] = 1e6 * df['Leakage current'].abs()
        return df
    
    def load_data(self, module_name: str) -> None:
        """
        Load and combine data for a specific module from all relevant files.
        
        Args:
            module_name: Name of the module to process
        """
        if module_name not in self.conditions:
            logger.error(f"Module {module_name} not found in conditions")
            raise ValueError(f"Module {module_name} not found")
        
        try:
            self.dataframes[module_name] = pd.DataFrame()
            for data_dir in self.data_dirs:
                files = self._get_files(data_dir)
                module_conditions = self.conditions[module_name]
                if len(files) > len(module_conditions):
                    logger.warning(
                        f"Mismatch in {data_dir}: {len(files)} files found, "
                        f"but {len(module_conditions)} conditions defined for {module_name}"
                    )
                for file_path, condition in zip(files, module_conditions):
                    df = self._load_file(file_path, condition)
                    df['Module'] = module_name
                    self.dataframes[module_name] = pd.concat(
                        [self.dataframes[module_name], df], ignore_index=True
                    )
            self.dataframes[module_name] = self._transform_data(self.dataframes[module_name])
            logger.info(f"Loaded data for {module_name}, shape: {self.dataframes[module_name].shape}")
        except Exception as e:
            logger.error(f"Error loading data for {module_name}: {e}")
            raise
    
    def analyze_data(self, module_name: str) -> Tuple[float, float, pd.Series, pd.Series]:
        """
        Analyze DataFrame for a module's min/max leakage current and corresponding rows.
        
        Args:
            module_name: Name of the module to analyze
            
        Returns:
            Tuple of min leakage, max leakage, and corresponding DataFrame rows
        """
        if module_name not in self.dataframes or self.dataframes[module_name].empty:
            logger.error(f"No data loaded for module {module_name}")
            raise ValueError(f"No data for {module_name}")
        
        df = self.dataframes[module_name]
        min_current = df['Leakage current'].min()
        max_current = df['Leakage current'].max()
        max_row = df.loc[df['Leakage current'].idxmax()]
        min_row = df.loc[df['Leakage current'].idxmin()]
        
        logger.info(f"Module {module_name} - Unique conditions: {df['Conditions'].unique()}")
        logger.info(f"Module {module_name} - Min leakage current: {min_current}")
        logger.info(f"Module {module_name} - Max leakage current: {max_current}")
        logger.debug(f"Module {module_name} - Max current row: {max_row.to_dict()}")
        logger.debug(f"Module {module_name} - Min current row: {min_row.to_dict()}")
        
        return min_current, max_current, max_row, min_row
    
    def plot_iv_curve(self, module_name: str, output_filename: str = None) -> None:
        """
        Plot IV curve for a module and save to file.
        
        Args:
            module_name: Name of the module to plot
            output_filename: Name of the output plot file (optional)
        """
        if module_name not in self.dataframes or self.dataframes[module_name].empty:
            logger.error(f"No data loaded for module {module_name}")
            raise ValueError(f"No data for {module_name}")
        
        try:
            output_path = self.data_dirs[0] / (
                output_filename or f'IV_curve_{module_name}_nonLog_zoomIn.png'
            )
            df = self.dataframes[module_name]
            plt.figure(figsize=(16, 12), dpi=300)
            for idx, condition in enumerate(df['Conditions'].unique()):
                df_subset = df[df['Conditions'] == condition]
                plt.plot(
                    df_subset['Bias voltage'],
                    df_subset['Leakage current'],
                    'o-',
                    label=condition,
                    color=self.COLORS[idx % len(self.COLORS)]
                )
            
            plt.xlabel('Bias Voltage (V)')
            plt.ylabel('Leakage Current (μA)')
            plt.xlim(0, 600)
            plt.ylim(0, 2.0)
            plt.grid(True)
            plt.legend()
            plt.title(f'IV Curve for {module_name}')
            plt.savefig(output_path, facecolor='w', dpi=300)
            plt.close()
            logger.info(f"Plot saved to {output_path}")
        except Exception as e:
            logger.error(f"Error generating plot for {module_name}: {e}")
            raise
    
    def process_module(self, module_name: str, output_filename: str = None) -> None:
        """
        Process and plot data for a specific module.
        
        Args:
            module_name: Name of the module to process
            output_filename: Name of the output plot file (optional)
        """
        try:
            self.load_data(module_name)
            self.analyze_data(module_name)
            self.plot_iv_curve(module_name, output_filename)
        except Exception as e:
            logger.error(f"Processing failed for {module_name}: {e}")
            raise
    
    def process_all_modules(self) -> None:
        """Process and plot data for all modules."""
        try:
            for module_name in self.conditions:
                self.process_module(module_name)
        except Exception as e:
            logger.error(f"Processing failed for all modules: {e}")
            raise

def main():
    """Main function to run the IV curve analysis for multiple modules."""
    data_dirs = [
        '/home/ruchi/hgcal/module_test/bias_supply_monitor/data_v3C_CMU_July1_2025/320-ML-F3TC-CM-0102',
        '/home/ruchi/hgcal/module_test/tests_of_modules_CMU_to_FNAL_Fall2024/fnal_zip/320-ML-F3CX-CM-0004'  # Replace with actual pickle data directory
    ]
    conditions = {
        '320-ML-F3TC-CM-0102': [
            'W/o dry air, RH:40.5', '15 mins, RH:0', '35 mins, RH:0', '60 mins, RH:0',
            '90 mins, RH:0', '140 mins, RH:0', '200 mins, RH:0', '240 mins, RH:0',
            '300 mins, RH:0', '320 mins, RH:0', '350 mins, RH:0', '360 mins, RH:3.2'
        ],
        'another-module': [
            'Condition1', 'Condition2'  # Example conditions for another module
        ]
    }
    
    analyzer = IVCurveAnalyzer(data_dirs, conditions)
    analyzer.process_all_modules()

if __name__ == '__main__':
    main()