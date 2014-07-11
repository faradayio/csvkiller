for f in $(ls $1/*/*.csv)
do
  NAME=$(basename $f)
  head -n 1 $f > $2/${NAME}
done

for f in $(ls $1/*/*.csv)
do
  echo "combining $f"
  NAME=$(basename $f)
  tail -n +2 $f >> $2/${NAME}
  rm $f;
done

rm -r $1