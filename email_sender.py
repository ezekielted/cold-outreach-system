import smtplib
import json
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(Path("venv/.env"))

def read_outreach_emails(json_path):
    """Read the outreach emails from JSON file"""
    try:
        with open(json_path, 'r', encoding='utf-8') as file:
            emails = json.load(file)
            print(f"Successfully loaded {len(emails)} emails from {json_path}")
            return emails
    except FileNotFoundError:
        print(f"Error: File not found at {json_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {json_path}")
        return None
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None

def format_html_body(body_text):
    """Format the plain text body into proper HTML"""
    # Split the text into paragraphs
    paragraphs = body_text.split('\n\n')
    
    # Format each paragraph with proper HTML
    html_paragraphs = []
    for paragraph in paragraphs:
        if paragraph.strip():
            # Handle signature section differently (after "Mit freundlichen Grüßen")
            if "Mit freundlichen Grüßen" in paragraph:
                # Split the signature part and format it
                sig_parts = paragraph.split('\n')
                sig_html = '<p>' + sig_parts[0] + '</p>'
                
                # Add the remaining signature lines with line breaks
                if len(sig_parts) > 1:
                    sig_html += '<p style="margin-top: 0;">'
                    sig_html += '<br>'.join(sig_parts[1:])
                    sig_html += '</p>'
                
                html_paragraphs.append(sig_html)
            else:
                # Regular paragraph
                formatted = '<p>' + paragraph.replace('\n', '<br>') + '</p>'
                html_paragraphs.append(formatted)
    
    # Join all paragraphs into a single HTML body
    html_body = '\n'.join(html_paragraphs)
    
    return html_body

def send_test_email(email_data, test_recipient):
    """Send a test email using the same SMTP configuration"""
    # Get SMTP settings from environment variables
    smtp_server = os.environ.get('SMTP_SERVER', 'smtp.mailersend.net')
    port = int(os.environ.get('SMTP_PORT', 587))
    username = os.environ.get('SMTP_USERNAME', '')
    password = os.environ.get('SMTP_PASSWORD', '')
    
    # Extract email data
    subject = "[TEST] " + email_data.get('subject', 'No Subject')
    from_email = email_data.get('from', username)
    body_text = email_data.get('body', '')
    
    # Create message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = test_recipient
    
    # Create the plain-text version
    part1 = MIMEText(body_text, 'plain', 'utf-8')
    
    # Create the HTML version with proper formatting
    html_content = f"""
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="UTF-8">
        <style>
          body {{ font-family: Arial, sans-serif; line-height: 1.5; color: #333; }}
          p {{ margin-bottom: 15px; }}
        </style>
      </head>
      <body>
        <div style="background-color: #f8f8f8; padding: 10px; margin-bottom: 20px; border-left: 4px solid #ff9900;">
          <strong>TEST EMAIL</strong> - This is a test email for the upcoming campaign. Original recipient would have been: {email_data.get('to', 'N/A')}
        </div>
        {format_html_body(body_text)}
      </body>
    </html>
    """
    part2 = MIMEText(html_content, 'html', 'utf-8')
    
    # Add HTML/plain-text parts to MIMEMultipart message
    msg.attach(part1)
    msg.attach(part2)
    
    try:
        # Create secure connection with server and send email
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()  # Secure the connection
        server.login(username, password)
        server.sendmail(from_email, test_recipient, msg.as_string())
        server.quit()
        print(f"✓ Test email successfully sent to {test_recipient}")
        print(f"  Subject: {subject}")
        return True
    except Exception as e:
        print(f"✗ Failed to send test email to {test_recipient}: {e}")
        return False

def main():
    """Send a test email before running the main campaign"""
    # Define the path to the JSON file using environment variable
    json_path = Path(os.environ.get('JSON_OUTPUT_PATH', "venv/data/outreach_emails.json"))
    
    # Define test recipient from environment variable
    test_recipient = os.environ.get('TEST_EMAIL_RECIPIENT', "team@propertyvisualizer.com")
    
    # Read the outreach emails
    emails = read_outreach_emails(json_path)
    if not emails or len(emails) == 0:
        print("No emails found to test with.")
        return
    
    print("\n" + "="*50)
    print("SENDING TEST EMAIL")
    print("="*50)
    
    # Select the first email as the test email
    test_email_data = emails[0]
    print(f"Using email template for: {test_email_data.get('to', 'N/A')}")
    print(f"Will be sent to test recipient: {test_recipient}")
    
    # Confirm with user
    while True:
        response = input("\nDo you want to send this test email? (yes/no): ").strip().lower()
        if response in ['yes', 'y']:
            break
        elif response in ['no', 'n']:
            print("Test email cancelled.")
            return
        else:
            print("Please enter 'yes' or 'no'.")
    
    # Send the test email
    success = send_test_email(test_email_data, test_recipient)
    
    if success:
        print("\nTest email sent successfully. Please check the inbox and verify content before proceeding with the main campaign.")
        
        # Ask if the user wants to continue to the main campaign
        while True:
            continue_response = input("\nProceed with the main email campaign? (yes/no): ").strip().lower()
            if continue_response in ['yes', 'y']:
                print("\nProceeding to main campaign...")
                # Import and call the main function from the original script
                from email_composer import main as run_main_campaign
                run_main_campaign()
                break
            elif continue_response in ['no', 'n']:
                print("Main campaign cancelled.")
                break
            else:
                print("Please enter 'yes' or 'no'.")
    else:
        print("\nTest email failed to send. Please check your SMTP settings and try again.")

if __name__ == "__main__":
    main()