for f in $1/*/*.csv
do
  ATTR=$(echo $f | awk -F '[/.]' '{print $(NF-1)}' -)
  head -n 1 $f > $2/${ATTR}.csv
done

for f in $1/*/*.csv
do
  echo "combining $f"
  ATTR=$(echo $f | awk -F '[/.]' '{print $(NF-1)}' -)
  tail -n +2 $f >> $2/${ATTR}.csv
  rm $f;
done

rm -r $1