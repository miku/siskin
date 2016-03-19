#/bin/bash

#  Copyright 2015 by Leipzig University Library, http://ub.uni-leipzig.de
#                    The Finc Authors, http://finc.info
#                    Martin Czygan, <martin.czygan@uni-leipzig.de>
#
# This file is part of some open source application.
#
# Some open source application is free software: you can redistribute
# it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# Some open source application is distributed in the hope that it will
# be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
# @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>

#
# Extract package-db table from GBI WISO XSLX.
#
# Usage: gbi_package_db_from_xlsx.sh FILE
#
# Where FILE is a XLSX spreadsheet, where column 14 is the database name.
#
# Example:
#
#     $ contrib/gbi_package_db_from_xlsx.sh package_a.xlsx | awk '{print "package_a\t"$0}' >> dbmap.tsv
#     $ contrib/gbi_package_db_from_xlsx.sh package_b.xlsx | awk '{print "package_b\t"$0}' >> dbmap.tsv
#     $ contrib/gbi_package_db_from_xlsx.sh package_c.xlsx | awk '{print "package_c\t"$0}' >> dbmap.tsv
#     ...
#
# SHA1 of various inputs:
#
# 4e4639b154434f8e97bfe2b9deb85ed9edf87ce8
# 2883eea50f718b5b205aee120af97a3d123aa04c
# f11c345d0583565cd1b5ff93e4dc11d832dc7e56
# 21d16fdccc21fadc61fb51d27c5967696be2dbf8
# a6d70e522e465b9cbeccaf390808d647e9565215

hash xlsx2csv 2> /dev/null || { echo >&2 "xlsx2csv is required (pip install xlsx2csv)"; exit 1; }
hash csvcut 2> /dev/null || { echo >&2 "csvkit is required (pip install csvkit)"; exit 1; }

if [ $# -eq 0 ]; then
    echo "Usage: $0 FILE"
    echo
    echo "FILE is WISO XLSX file with database names in column 14."
    exit 1
fi

xlsx2csv "$1" | tail -n +2 | csvcut -c 14
