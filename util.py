def removeDuplicate(df):
    dfDuplicateSum=df.duplicated().sum()
    print('Duplicates ',dfDuplicateSum)
    if dfDuplicateSum>0:
        df.drop_duplicates(inplace=True)

def get_month_diff(df):
    # Calculate difference in years and months
    year_diff = df['order_month'].dt.year - df['cohort_month'].dt.year
    month_diff = df['order_month'].dt.month - df['cohort_month'].dt.month
    return year_diff * 12 + month_diff

def assign_segment(df):
    r = df['R_Score']
    f = df['F_Score']
    
    if r >= 4 and f >= 4:
        return 'Champions'
    elif r >= 3 and f >= 3:
        return 'Loyal Customers'
    elif r >= 4 and f < 3:
        return 'New Customers'
    elif r <= 2 and f >= 4:
        return 'At Risk / Can\'t Lose'
    elif r <= 2 and f <= 2:
        return 'Lost / Hibernating'
    else:
        return 'About to Sleep'