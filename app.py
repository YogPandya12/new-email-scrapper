from flask import Flask, request, render_template, send_file
import pandas as pd
from io import BytesIO
from concurrent.futures import ProcessPoolExecutor
from scrapper import find_emails
import logging
import multiprocessing

app = Flask(__name__)
application = app

# Configure logging
logging.basicConfig(level=logging.DEBUG)
app.logger.setLevel(logging.DEBUG)

def find_url_column(columns):
    """Identify the column likely containing URLs."""
    keywords = ['website', 'url', 'websites', 'urls']
    for col in columns:
        if any(keyword in col.lower() for keyword in keywords):
            return col
    return None

def get_optimal_workers(file_size):
    """Return optimal number of workers based on CPU and file size."""
    cpu_count = multiprocessing.cpu_count()
    if file_size <= 100:
        return min(cpu_count, 4, file_size)  # Cap at 4 or CPU count
    elif file_size <= 300:
        return min(cpu_count, 8, file_size)
    else:
        return min(cpu_count * 2, 16, file_size)  # Scale with CPUs, cap at 16

def worker(url):
    """Process a single URL and extract emails."""
    try:
        print(f"Processing URL: {url}")
        emails = find_emails(url)
        print(f"Processed {url}: {emails}")
        return emails
    except Exception as e:
        print(f"Error processing URL {url}: {str(e)}")
        return []

def process_urls_in_parallel(df, url_column, num_workers):
    """Process all URLs in parallel using ProcessPoolExecutor."""
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = list(executor.map(worker, df[url_column]))
    return results

@app.route('/')
def upload_file():
    app.logger.info('Home route accessed.')
    return render_template('upload.html')

@app.route('/process', methods=['POST'])
def process_file():
    app.logger.info('Processing file upload.')
    file = request.files.get('file')

    if not file or not file.filename.endswith('.xlsx'):
        app.logger.error('Invalid file type or no file uploaded.')
        return "Invalid file type. Please upload an Excel file.", 400

    try:
        app.logger.info('Reading Excel file.')
        df = pd.read_excel(file)
        
        url_column = find_url_column(df.columns)
        if not url_column:
            app.logger.error('No URL column found.')
            return "No column found that likely contains URLs.", 400
        
        df[url_column] = df[url_column].astype(str).replace('nan', '')
        
        num_workers = get_optimal_workers(len(df))
        app.logger.info(f'Processing {len(df)} URLs with {num_workers} workers.')
        
        df['Emails'] = process_urls_in_parallel(df, url_column, num_workers)
        
        app.logger.info('Writing to Excel.')
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)

        original_filename = file.filename
        processed_filename = f"{original_filename}"

        app.logger.info(f'Sending file: {processed_filename}')
        return send_file(
            output,
            as_attachment=True,
            download_name=processed_filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        app.logger.error(f"Error occurred: {str(e)}", exc_info=True)
        return f"An error occurred: {str(e)}", 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)