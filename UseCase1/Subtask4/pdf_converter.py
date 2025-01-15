import markdown
import pdfkit
import os


def convert_md_to_html(md_file, output_html):
    # Read the markdown file
    with open(md_file, 'r', encoding='utf-8') as file:
        md_content = file.read()

    # Convert markdown to HTML
    html_content = markdown.markdown(md_content)

    # Write the HTML content to a file
    with open(output_html, 'w', encoding='utf-8') as file:
        file.write(html_content)


def convert_html_to_pdf(html_file, output_pdf):
    # Convert HTML to PDF
    pdfkit.from_file(html_file, output_pdf)


def main():
    md_file = './README.md'  # Path to the markdown file
    output_html = './Task4.html'  # Path to the intermediate HTML file
    output_pdf = './Task4.pdf'  # Path to the final PDF file

    convert_md_to_html(md_file, output_html)
    # convert_html_to_pdf(output_html, output_pdf)
    #
    # # Clean up the intermediate HTML file
    # os.remove(output_html)
    print(f'PDF generated successfully: {output_pdf}')


if __name__ == '__main__':
    main()