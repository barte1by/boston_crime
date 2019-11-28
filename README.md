# boston_crime

####To run:
spark-submit --master local[*] --class com.example.boston_crimes target/scala-2.11/BostonCrimesMap-assembly-1.0.jar src/files/crime.csv src/files/offense_codes.csv src/files/result