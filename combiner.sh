SAVEIFS=$IFS
IFS=$(echo -en "\n\b")

for f in $(ls -1 $1/*/*.csv)
do
  NAME=$(basename $f)
  head -n 1 $f > $2/${NAME}
done

for f in $(ls -1 $1/*/*.csv)
do
  echo "combining $f"
  NAME=$(basename $f)
  tail -n +2 $f >> $2/${NAME}
  tail -c1 $2/${NAME} | read -r _ || echo >> $2/${NAME}
  rm $f;
done

rm -r $1

IFS=$SAVEIFS
