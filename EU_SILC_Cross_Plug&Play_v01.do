* Define main working directories
clear
cd "C:/RData_DPhil/DECIPHE Project" // current directory for charts and analyses
global base_dir "C:/RData_DPhil/EUROSTAT/EU-SILC/Cross_23_09/Cross" // directory where you have downloaded EU-SILC cross-sectional data
global dirdata "C:/RData_DPhil/EUROSTAT/EU-SILC/Cross_23_09/Cross_dta_files" // directory where you will save all datasets

***********************************************************************
* Provide key inputs

* Select countries and years you are interested in
global countries "AT BE BG CH CY CZ DE DK EE EL ES FI FR HR HU IE IS IT LT LU LV MT NL NO PL PT RO RS SE SI SK UK"
global years "04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22"

* Select variables of interest for databse R, P, D, and H
* REMARK: merging variables including year, country, pid/hid MUST be included
global selvarR "rb010 rb020 rb030 rb050 rb080 rb081 rb082 rb090 rb200 rb245 rb250 rb220 rb220_f rb230 rb230_f rb240 rb240_f rb211" // year, country, pid, cross sectional weight, year of birth, age (end of income), age (end of int), sex, residential
global selvarP "pb010 pb020 pb030 px030 pb040 pt210 pt210_f " // pb010 year, pb020 country, px030 hid, pb030 pid ;  pb040 cross-sectional individual weights all household members above 16 | px020 age at the end of the survey, pb110 year of birth, pb150 sex | pt210 parental status , pt210_f parental status flag | pb170 pb180 pb190 motherid, fatherid, partnerid
global selvarD "db010 db020 db030 db040 db040_f db050 db060 db062 db070 db090" // year, country, hid, region of residence, stratum, psu, ssu, order of selection of PSU, cross sectional hh weight
global selvarH "hb010 hb020 hb030 hb070 hh020 hh021" // year, country, hid, respondentID, hh tenure before 2010, hhtenure after 2010


* Provide renaming of any variable you want to make more transparent
* REMARK: year country pid and hid MUST be renamed to allow merging"
global oldnamesR "rb010 rb020 rb030 rb050 rb080 rb090 rb200 rb245 rb250 rb211 rb220 rb220_f rb230 rb230_f rb240 rb240_f rb081 rb082"
global newnamesR "year country pid cweight cohort sex residence_status respondent_status data_status main_status fatherid fatherid_f motherid motherid_f partnerid partnerid_f age age_income_year"

global oldnamesP "pb010 pb020 pb030 px030 pb040 pt210 pt210_f"
global newnamesP "year country pid hid cweight_p parental_housingten parental_housingten_f"

global oldnamesD "db010 db020 db030 db040 db040_f db050 db060 db062 db070 db090"
global newnamesD "year country hid residence residence_f stratum psu ssu su_selection_order hh_cweights"

global oldnamesH "hb010 hb020 hb030 hb070"
global newnamesH "year country hid hrp"

clear










***********************************************************************
* Move all individual datafiles to a unique reference directory - named dirdata

* Step 1: Identify data directories
local base_dir "${base_dir}"
local base_name "UDB_c"
local countries "${countries}"
local years "${years}"
local suffix "D H P R"
local save_dir "${dirdata}"

* Step 2: Loop over the countries, years, and suffixes
foreach country in `countries' {
    foreach year in `years' {
        foreach l in `suffix' {
            local file_name = "`base_dir'/`country'/20`year'/UDB_c`country'`year'`l'.csv"
			
			* Use capture to avoid loop interruption if file is not found
            capture {
                import delimited "`file_name'", clear

                * Step 3: Save each imported file as a .dta file in the save_dir
                local save_name = "`save_dir'/UDB_c`country'`year'_`l'.dta"
                save "`save_name'", replace
            }
			
			* Optionally: check if the file was successfully imported and log if it wasn't
            if (_rc != 0) {
                display as error "File `file_name' not found or could not be imported."
			}
        }
    }
}







***********************************************************************
* R- Registry file

foreach x of global countries {
	
	foreach y of global years {
		
		capture {
			use "${dirdata}/UDB_c`x'`y'_R.dta", clear
			
			* define vars of interest	
			local vars ${selvarR}
			* Initialize an empty list of existing variables
			local existing_vars
			* Loop through each variable and check if it exists
			foreach var of local vars {
				capture confirm variable `var'
				if !_rc {  // If the variable exists, add it to the list
				local existing_vars `existing_vars' `var'
				}
				}
				
				* Keep only the variables that exist
				if "`existing_vars'" != "" {
					keep `existing_vars'
					}
				
				* use only selected variables
				use `existing_vars' using "${dirdata}/UDB_c`x'`y'_R.dta", clear 
				save "${dirdata}/UDB_c`x'`y'_Rtemp.dta", replace 
		}
		
	}
	
}


clear 
* use file list to create a dataset including the directory and the name of each file ending with _Ptemp.dta
filelist, dir("${dirdata}/") pattern("*_Rtemp.dta") save("filelist_SILC_Rtemp.dta") replace
use "filelist_SILC_Rtemp.dta", clear
local fn1 = filename[1]
local dn1 = dirname[1]
global first_file = "`dn1'`fn1'"

* create local variables including the directory and name of all files identified from 1 to N
use "filelist_SILC_Rtemp.dta", clear
local obs = _N
forvalues i=2/`obs' {
	local fni = filename[`i']
	local dni = dirname[`i']
	local filenumber`i' = "`dni'`fni'"
	}
	
use "$first_file", clear
         forvalues i=2/`obs' {
            append using `filenumber`i'', force 
         }	
save "${dirdata}/UDB_c_Rsel.dta", replace

* rename key variables and save
use "${dirdata}/UDB_c_Rsel.dta", clear
rename (${oldnamesR}) (${newnamesR})
save "${dirdata}/UDB_c_Rsel.dta", replace
clear











*****************************************************************
* P - Personal file 

foreach x of global countries  {
	foreach y of global years {
		
		capture {
			use "${dirdata}/UDB_c`x'`y'_P.dta", clear
			
			* define vars of interest	
			local vars ${selvarP}
			* Initialize an empty list of existing variables
			local existing_vars
			* Loop through each variable and check if it exists
			foreach var of local vars {
				capture confirm variable `var'
				if !_rc {  // If the variable exists, add it to the list
				local existing_vars `existing_vars' `var'
				}
				}
				
				* Keep only the variables that exist
				if "`existing_vars'" != "" {
					keep `existing_vars'
					}
				
				* use only selected variables
				use `existing_vars' using "${dirdata}/UDB_c`x'`y'_P.dta", clear 
				
				* transform pb020 format to allow for append
				
				save "${dirdata}/UDB_c`x'`y'_Ptemp.dta", replace 
		}
		
	}
}




clear 
* use file list to create a dataset including the directory and the name of each file ending with _Ptemp.dta
filelist, dir("${dirdata}/") pattern("*_Ptemp.dta") save("filelist_SILC_Ptemp.dta") replace
use "filelist_SILC_Ptemp.dta", clear
local fn1 = filename[1]
local dn1 = dirname[1]
global first_file = "`dn1'`fn1'"

* create local variables including the directory and name of all files identified from 1 to N
use "filelist_SILC_Ptemp.dta", clear
local obs = _N
forvalues i=2/`obs' {
	local fni = filename[`i']
	local dni = dirname[`i']
	local filenumber`i' = "`dni'`fni'"
	}
	
use "$first_file", clear
         forvalues i=2/`obs' {
            append using `filenumber`i'', force 
         }	
save "${dirdata}/UDB_c_Psel.dta", replace

* rename key variables
use "${dirdata}/UDB_c_Psel.dta", clear
rename (${oldnamesP}) (${newnamesP})
save "${dirdata}/UDB_c_Psel.dta", replace
clear









*********************************************************
* D - Household Registry file - survey Design variables 

foreach x of global countries {
	foreach y of global years {
		
		capture {
			use "${dirdata}/UDB_c`x'`y'_D.dta", clear
			
			* define vars of interest	
			local vars ${selvarD}
			* Initialize an empty list of existing variables
			local existing_vars
			* Loop through each variable and check if it exists
			foreach var of local vars {
				capture confirm variable `var'
				if !_rc {  // If the variable exists, add it to the list
				local existing_vars `existing_vars' `var'
				}
				}
				
				* Keep only the variables that exist
				if "`existing_vars'" != "" {
					keep `existing_vars'
					}
				
				* use only selected variables
				use `existing_vars' using "${dirdata}/UDB_c`x'`y'_D.dta", clear 
				save "${dirdata}/UDB_c`x'`y'_Dtemp.dta", replace 
		}
	}
}

clear
* use file list to create a dataset including the directory and the name of each file ending with _Ptemp.dta
filelist, dir("${dirdata}/") pattern("*_Dtemp.dta") save("filelist_SILC_Dtemp.dta") replace
use "filelist_SILC_Dtemp.dta", clear
local fn1 = filename[1]
local dn1 = dirname[1]
global first_file = "`dn1'`fn1'"

* create local variables including the directory and name of all files identified from 1 to N
use "filelist_SILC_Dtemp.dta", clear
local obs = _N
forvalues i=2/`obs' {
	local fni = filename[`i']
	local dni = dirname[`i']
	local filenumber`i' = "`dni'`fni'"
	}
	
use "$first_file", clear
         forvalues i=2/`obs' {
            append using `filenumber`i'', force 
         }	
save "${dirdata}/UDB_c_Dsel.dta", replace

* rename key variables and save
use "${dirdata}/UDB_c_Dsel.dta", clear
rename (${oldnamesD}) (${newnamesD})
save "${dirdata}/UDB_c_Dsel.dta", replace
clear








*********************************************************
* H - Household Response file 

foreach x of global countries {
	foreach y of global years{
		
		capture {
			use "${dirdata}/UDB_c`x'`y'_H.dta", clear
			
			* define vars of interest	
			local vars ${selvarH}
			* Initialize an empty list of existing variables
			local existing_vars
			* Loop through each variable and check if it exists
			foreach var of local vars {
				capture confirm variable `var'
				if !_rc {  // If the variable exists, add it to the list
				local existing_vars `existing_vars' `var'
				}
				}
				
				* Keep only the variables that exist
				if "`existing_vars'" != "" {
					keep `existing_vars'
					}
				
				* use only selected variables
				use `existing_vars' using "${dirdata}/UDB_c`x'`y'_H.dta", clear 
				save "${dirdata}/UDB_c`x'`y'_Htemp.dta", replace 
		}
		
	}
}

clear
* use file list to create a dataset including the directory and the name of each file ending with _Ptemp.dta
filelist, dir("${dirdata}/") pattern("*_Htemp.dta") save("filelist_SILC_Htemp.dta") replace
use "filelist_SILC_Htemp.dta", clear
local fn1 = filename[1]
local dn1 = dirname[1]
global first_file = "`dn1'`fn1'"

* create local variables including the directory and name of all files identified from 1 to N
use "filelist_SILC_Htemp.dta", clear
local obs = _N
forvalues i=2/`obs' {
	local fni = filename[`i']
	local dni = dirname[`i']
	local filenumber`i' = "`dni'`fni'"
	}
	
use "$first_file", clear
         forvalues i=2/`obs' {
            append using `filenumber`i'', force 
         }	
save "${dirdata}/UDB_c_Hsel.dta", replace

* rename key vars
use "${dirdata}/UDB_c_Hsel.dta", clear
rename (${oldnamesH}) (${newnamesH})
save "${dirdata}/UDB_c_Hsel.dta", replace
clear





**************************************************
* MERGE R - P - D - H

use "${dirdata}/UDB_c_Rsel.dta", clear
use "${dirdata}/UDB_c_Psel.dta", clear
use "${dirdata}/UDB_c_Dsel.dta", clear
use "${dirdata}/UDB_c_Hsel.dta", clear


use "${dirdata}/UDB_c_Rsel.dta", clear
merge 1:1 year country pid using "${dirdata}/UDB_c_Psel.dta", force
rename _merge merge_RtoP

merge m:1 year country hid using "${dirdata}/UDB_c_Dsel.dta", force
rename _merge merge_RtoD

merge m:1 year country hid using "${dirdata}/UDB_c_Hsel.dta", force
rename _merge merge_RtoH

save "${dirdata}/UDB_c_RPDHsel.dta", replace






