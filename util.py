def removeDuplicate(df):
    dfDuplicateSum=df.duplicated().sum()
    print('Duplicates ',dfDuplicateSum)
    if dfDuplicateSum>0:
        df.drop_duplicates(inplace=True)