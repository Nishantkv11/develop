from app import SearchKeywordRevenueProcessor

if __name__ == "__main__":
    input_file = "sample data.sql"   # change to your file name if needed
    processor = SearchKeywordRevenueProcessor(input_file)
    output_path = processor.write_output(".")
    print("Output file:", output_path)
    print(processor.summary())