# app.py
import streamlit as st
import Dan_functions as fn

def main():
    st.title('Supermarket Sales Dashboard')

    demo_path = '../Data/Raw/df_final_demo (1).txt'
    client_path = '../Data/Raw/df_final_experiment_clients.txt'
    web_path1 = '../Data/Raw/df_final_web_data_pt_1.txt'
    web_path2 = '../Data/Raw/df_final_web_data_pt_2.txt'

    final_df = fn.load_merge_date(demo_path, client_path, web_path1, web_path2)
    fn.clean_data(final_df)
    
    # Interactive widgets
    st.sidebar.header('Controls')
    min_rating = st.sidebar.slider('Minimum Rating', min_value=0, max_value=10, value=5, step=1)
    
    # Filter by rating
    # filtered_data = final_df[final_df['Rating'] >= min_rating]

    # Summary statistics
    # updated_summary = get_summary(filtered_data)
    st.write("### Summary Statistics")
    st.table(final_df)

    # Display raw data
    st.write("### Raw Data")
    st.dataframe(final_df)

    # Plotting
    #st.write("### Sales Over Time")
    #plt = fn.generate_account_plot(final_df)
    #st.pyplot(plt)
    
if __name__ == '__main__':
    main()