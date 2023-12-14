import os
import re
import sys
import dataclasses
import configparser
from datetime import date
from typing import Optional

import pandas as pd

from utils import Logger

config = configparser.ConfigParser()

with open("./settings/settings.ini", "r") as file:
    config.read_file(file)

OUTPUT_PATH = config.get("paths", "output_path")

INPUT_NEW_PATH = config.get("paths", "new_excel_files_path")

INPUT_OLD_PATH = config.get("paths", "old_excel_files_path")

COLUMNS = ["Title", "Url", "Image", "Price"]

DOMAIN_RE = re.compile(r"([a-zA-Z0-9\.\-]+(?![\/]))[\w\-\.]*\.(csv|xlsx)$", re.I)

@dataclasses.dataclass
class FileStats:
    """Store file descriptions i.e. file name, products before and products after"""
    file_name: str
    products_count_before: int
    products_count_after: Optional[int] = None
    error: Optional[str] = None

@dataclasses.dataclass
class SameDomainFiles:
    """Stores old files from the same domain"""
    domain: str
    file_paths: list[str] = dataclasses.field(default_factory=list)

@dataclasses.dataclass
class Columns:
    """stores columns information"""
    title: str
    url: str
    image: str
    price: str

class Deduplicator:
    """Removes duplicates from new files based on old files"""
    def __init__(self) -> None:
        self.logger = Logger(__class__.__name__)
        self.logger.info("*****Excel Deduplicator started*****")

        self.file_stats: list[FileStats] = []
        self.dataframes: list[pd.DataFrame] = []

        self.old_files = self.__get_files(INPUT_OLD_PATH)
        self.new_files = self.__get_files(INPUT_NEW_PATH, "new")

    def __get_files(self, path: str, key: Optional[str]="old") -> Optional[list[str]]:
        files = [f"{path}{file}" for file in os.listdir(path) 
                 if re.search(r"(csv|xlsx)$", file, re.I)]
        
        if len(files):
            info_msg = "files" if len(files) > 1 else "file"

            self.logger.info(f"{len(files)} {key} {info_msg} found")
            return files
        
        self.logger.error("No excel/csv files found in {} directory!".format(key))
    
    def __get_matching_files(self, new_file: str, name: str) -> SameDomainFiles:
        domain = DOMAIN_RE.search(new_file).group(1)

        same_domain_files = SameDomainFiles(domain=domain)

        for file in self.old_files:
            if re.search(rf"{domain}", file, re.I):
                same_domain_files.file_paths.append(file)
        
        matching_files = len(same_domain_files.file_paths)

        self.logger.info(f"{matching_files} matches found for {name}")

        return same_domain_files

    @staticmethod
    def __get_columns(columns: list[str], stats: Optional[FileStats]=None) -> Optional[Columns]: 
        for column in columns:
            if re.search(r"title", column, re.I):
                title = column
            elif re.search(r"url", column, re.I):
                url = column
            elif re.search(r"image", column, re.I):
                image = column
            elif re.search(r"price", column, re.I):
                price = column
        
        try:
            return Columns(title=title, url=url, image=image, price=price)
        except: 

            if stats is None: return
            
            _, error, _ = sys.exc_info()

            stats.error = f"missing column! {error}"

    @staticmethod
    def __rename_columns(df: pd.DataFrame, columns: Columns) -> pd.DataFrame:
        return df.rename(columns={columns.title: COLUMNS[0],
                                  columns.url: COLUMNS[1],
                                  columns.image: COLUMNS[2],
                                  columns.price: COLUMNS[-1]})
    
    @staticmethod
    def __read_file(file_path: str) -> Optional[pd.DataFrame]:
        if re.search(r".xlsx$", file_path, re.I):
            return pd.read_excel(file_path)
        elif re.search(r".csv$", file_path, re.I):
            return pd.read_csv(file_path)
    
    @staticmethod
    def __remove_whitespace(df: pd.DataFrame) -> None:
        df[COLUMNS[-1]] = df[COLUMNS[-1]].apply(
            lambda value: str(value).strip()
        )

        df[COLUMNS[0]] = df[COLUMNS[0]].apply(
            lambda value: str(value).strip()
        )
    
    @staticmethod
    def __format_price(df: pd.DataFrame) -> pd.DataFrame:
        df.dropna(subset=COLUMNS[-1], inplace=True)

        df = df.astype({COLUMNS[-1]: str})

        df = df.loc[~df[COLUMNS[-1]].str.contains("-")]

        df = df.loc[df[COLUMNS[-1]].str.contains(r"\$*\d+\.*\d*(?![\-])", regex=True)]

        df.loc[df[COLUMNS[-1]].str.contains(r"\d*\s*", regex=True), 
               COLUMNS[-1]] = df[COLUMNS[-1]].apply(
            lambda value: str(value).strip("$").strip().replace(",", "").replace(" ", "")
        )

        df.loc[df[COLUMNS[-1]].str.contains(r"\d*\s*", regex=True), 
               COLUMNS[-1]] = df[COLUMNS[-1]].apply(
            lambda value: re.search(r"\d+.?\d*", str(value)).group()
        )

        df[COLUMNS[-1]] = df[COLUMNS[-1]].astype(float)

        df[COLUMNS[-1]] =  df[COLUMNS[-1]].map("${:,.2f}".format)

        return df
    
    def __drop_duplicates(self, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        df1 = df1.dropna(subset=[COLUMNS[0], COLUMNS[-1]]).astype({COLUMNS[-1]: str})

        df2 = df2.dropna(subset=[COLUMNS[0], COLUMNS[-1]]).astype({COLUMNS[-1]: str})

        merged = df1.merge(df2[[COLUMNS[0], COLUMNS[-1]]], 
                           on=[COLUMNS[0], COLUMNS[-1]], 
                           how="left", 
                           indicator=True).query('_merge == "left_only"')
        
        return merged.drop(columns="_merge")
    
    def __process_matching_files(self, 
                                 df: pd.DataFrame, 
                                 files: SameDomainFiles) -> pd.DataFrame:
        for file in files.file_paths:
            self.logger.info("Comparing to {}".format(file.split(INPUT_OLD_PATH)[-1]))

            df_old = self.__read_file(file)

            columns = self.__get_columns(df_old.columns.values)

            if columns is None: continue

            df_old = self.__rename_columns(df_old, columns)

            self.__remove_whitespace(df_old)

            df_old = self.__format_price(df_old)

            df = self.__drop_duplicates(df, df_old)

            self.logger.info(
                f"Domain: {files.domain} || Products remaining: {len(df)}")
            
            if not len(df): break
        
        return df
    
    @staticmethod
    def __str_to_float(value: str) -> Optional[float]:
        if value:
            if isinstance(value, float): return value

            if isinstance(value, int): return float(value)

            if isinstance(value, str):
                return float(value.replace(",", "").replace(" ", "").replace("$", "").strip())
    
    def __combine_price_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        price_columns = [c for c in df.columns.values if re.search("price", c, re.I)]

        if len(price_columns) > 1:
            for col in price_columns:
                df[col] = df[col].apply(lambda value: self.__str_to_float(value))
            
            df["Price"] = df[price_columns].min(axis=1)

            [df.drop(columns=c, inplace=True) for c in price_columns if c != "Price"]
        
        return df
    
    def __save_to_csv(self, df: pd.DataFrame, filename: str) -> None:
        filename = filename.replace(".xlsx", ".csv").replace(".csv", "_filtered.csv")

        df.to_csv(f"{OUTPUT_PATH}{filename}", index=False)

        self.logger.info(f"{len(df)} products saved to {filename}")

    def run(self) -> None:
        for file in self.new_files:
            name = file.split(INPUT_NEW_PATH)[-1]

            self.logger.info("Processing file: {}".format(name))

            df = self.__read_file(file)

            stats = FileStats(file_name=name, products_count_before=len(df))

            if df is None: continue

            df = self.__combine_price_columns(df)

            columns = self.__get_columns(df.columns.values, stats)

            if columns is None: 
                stats.products_count_before = None

                self.file_stats.append(file_stats)

                continue

            df = self.__rename_columns(df, columns)

            self.__remove_whitespace(df)

            df = self.__format_price(df)

            oldfiles = self.__get_matching_files(file, name)

            df = self.__process_matching_files(df, oldfiles)

            stats.products_count_after = len(df)   

            if len(df):
                self.__save_to_csv(df, filename=name)   

                self.dataframes.append(df)

            else: 
                self.logger.info("No unique products from file: {}".format(name))

            self.file_stats.append(stats)

        file_stats = [dataclasses.asdict(stats) for stats in self.file_stats] 

        pd.DataFrame(file_stats).to_csv("./stats/stats.csv", index=False)

        self.logger.info("Done filtering files.")   


if __name__ == "__main__":
    app = Deduplicator()
    app.run()
