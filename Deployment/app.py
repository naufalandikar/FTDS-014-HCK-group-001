import numpy as np
import pandas as pd
from PIL import Image
from PIL import ImageFile
import urllib.request
from sklearn.metrics import pairwise_distances
from datetime import datetime
import streamlit as st


st.set_option('deprecation.showfileUploaderEncoding', False)

fashion_df = pd.read_csv("./fashion.csv")
boys_extracted_features = np.load('./Boys_ResNet_features.npy')
boys_Productids = np.load('./Boys_ResNet_feature_product_ids.npy')
girls_extracted_features = np.load('./Girls_ResNet_features.npy')
girls_Productids = np.load('./Girls_ResNet_feature_product_ids.npy')
men_extracted_features = np.load('./Men_ResNet_features.npy')
men_Productids = np.load('./Men_ResNet_feature_product_ids.npy')
women_extracted_features = np.load('./Women_ResNet_features.npy')
women_Productids = np.load('./Women_ResNet_feature_product_ids.npy')
fashion_df["ProductId"] = fashion_df["ProductId"].astype(str)

st.image("https://storage.googleapis.com/danacita-website-v3-prd/website_v3/images/biaya_bootcamp__kursus_hacktiv8_6.original.png")
st.markdown('---')
st.subheader('FashClass - HCK-14 Final Project') 
st.write('Name :')
st.write('1. Anjas Fajar Maulana    (Data Science)')
st.write('2. Fazrin Muhammad    (Data Analyst)')
st.write('3. Naufal Andika Ramadhan (Data Engineer)')
st.write('4. Salsa Sabitha Hurriyah (Data Science)')

st.write('---')
def load_data(file_path):
    return pd.read_csv(file_path)

# Path to the CSV file
file_path = "fashion.csv"
# Load the data
data = load_data(file_path)

# Display the data using Streamlit
st.write("### List of Product")
# Create a button to show/hide the data
if st.button("Show Data"):
    st.write(data)
    
st.write('---')
# Define function to filter dataset based on gender
def filter_dataset_by_gender(data, gender_filter):
    filtered_data = data[data['Gender'].str.contains(gender_filter, case=False)]
    return filtered_data

st.write("### Filter")
# Create a text_input for filtering by gender
gender_filter = st.selectbox("Filter by gender", ["Boys", "Girls", "Men", "Women"])

# Filter the dataset based on the input gender filter
filtered_data = filter_dataset_by_gender(data, gender_filter)

# Display the filtered dataset
st.write(filtered_data)

st.write('---')

def get_similar_products_cnn(product_id, num_results):
    if product_id not in fashion_df['ProductId'].values:
        st.write("❌ Product ID is not valid")
        return
    if(fashion_df[fashion_df['ProductId']==product_id]['Gender'].values[0]=="Boys"):
        extracted_features = boys_extracted_features
        Productids = boys_Productids
    elif(fashion_df[fashion_df['ProductId']==product_id]['Gender'].values[0]=="Girls"):
        extracted_features = girls_extracted_features
        Productids = girls_Productids
    elif(fashion_df[fashion_df['ProductId']==product_id]['Gender'].values[0]=="Men"):
        extracted_features = men_extracted_features
        Productids = men_Productids
    elif(fashion_df[fashion_df['ProductId']==product_id]['Gender'].values[0]=="Women"):
        extracted_features = women_extracted_features
        Productids = women_Productids
    Productids = list(Productids)
    doc_id = Productids.index(product_id)
    pairwise_dist = pairwise_distances(extracted_features, extracted_features[doc_id].reshape(1,-1))
    indices = np.argsort(pairwise_dist.flatten())[0:num_results+1]
    pdists  = np.sort(pairwise_dist.flatten())[0:num_results+1]
    st.write("""
         #### input item details
         """)
    ip_row = fashion_df[['ImageURL','ProductTitle']].loc[fashion_df['ProductId']==Productids[indices[0]]]
    for indx, row in ip_row.iterrows():
        image = Image.open(urllib.request.urlopen(row['ImageURL']))
        image = image.resize((224,224))
        st.image(image)
        st.write(f"Product Title: {row['ProductTitle']}")
    st.write(f"""
         #### Top {num_results} Recommended items
         """)
    for i in range(1,len(indices)):
        rows = fashion_df[['Gender','ImageURL','ProductTitle','SubCategory']].loc[fashion_df['ProductId']==Productids[indices[i]]]
        for indx, row in rows.iterrows():
            #image = Image.open(Image(url=row['ImageURL'], width = 224, height = 224,embed=True))
            image = Image.open(urllib.request.urlopen(row['ImageURL']))
            image = image.resize((224,224))
            st.image(image)
            st.write(f"Gender Class: {row['Gender']}")
            st.write(f"Sub Category: {row['SubCategory']}")
            st.write(f"Product Title: {row['ProductTitle']}")
            #st.write(f"Euclidean Distance from input image: {pdists[i]}")

st.write("""
         ## FashClass Recommendation
         """
         )

user_input1 = st.text_input("Enter the item id")
user_input2 = st.text_input("Enter number of products to be recommended")

button = st.button('Generate recommendations')
st.write('---')
if button:
    get_similar_products_cnn(str(user_input1), int(user_input2))