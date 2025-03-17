import csv
import json
import requests
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

class TokenRateLimiter:
    """Manages API rate limiting based on token usage"""
    def __init__(self, tokens_per_minute=6000):
        self.tokens_per_minute = tokens_per_minute
        self.used_tokens = 0
        self.window_start = datetime.now()
    
    def request_permission(self, estimated_tokens=1500):
        """
        Check if a request with estimated token usage can be made
        Returns: seconds to wait (0 if no wait needed)
        """
        now = datetime.now()
        
        # Reset counter if a minute has passed
        if now - self.window_start > timedelta(minutes=1):
            self.used_tokens = 0
            self.window_start = now
            return 0
        
        # Calculate remaining tokens in current window
        remaining_tokens = self.tokens_per_minute - self.used_tokens
        
        # If we have enough tokens, grant permission immediately
        if remaining_tokens >= estimated_tokens:
            return 0
        
        # Calculate time until next window
        seconds_until_reset = 60 - (now - self.window_start).seconds
        return seconds_until_reset
    
    def record_usage(self, tokens_used):
        """Record actual token usage after making a request"""
        self.used_tokens += tokens_used

def read_leads_from_csv(file_path):
    """Read lead data from CSV file"""
    leads = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            leads.append(row)
    return leads

def generate_email_with_groq(lead, rate_limiter):
    # API configuration
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {os.getenv('GROQ_API_KEY')}",
        "Content-Type": "application/json"
    }
    
    # Construct the prompt with lead data - now in German
    prompt = f"""Generiere eine direkte, professionelle Kalt-E-Mail auf Deutsch für {lead.get('name', '')}.

Geschäftsdetails:
- Firmenname: {lead.get('name', '')}
- Name des Inhabers: {lead.get('owner_name', '')}
- Adresse: {lead.get('full_address', '')}
- Geschäftstyp: {lead.get('type', '')}
- Bewertung: {lead.get('rating', '')} von 5 Sternen aus {lead.get('review_count', '')} Bewertungen
- Verifizierungsstatus: {lead.get('verified', '')}
- Status: {lead.get('business_status', '')}
- Zusätzliche Informationen: {lead.get('about', '')}

Unsere Unternehmensdetails:
- Firmenname: ExposeProfi
- Website: http://exposeprofi.de/
- Dienstleistungen: 3D-Architekturvisualisierungen, Immobiliendesign
- Wertversprechen: Wir unterstützen Unternehmen, Agenturen, Entwickler und Immobilienprofis mit fotorealistischen Visualisierungen, die die Vermarktung ihrer Projekte transformieren und ihre Verkaufskonversionsraten deutlich steigern.

FORMAT-ANWEISUNGEN:
- Erstelle eine einzigartige und ansprechende Betreffzeile, die auf den spezifischen Geschäftstyp und die Bedürfnisse zugeschnitten ist
- Beginne mit "Betreff: [Deine dynamische Betreffzeile hier]"
- Fahre mit einer angemessenen E-Mail-Anrede fort (z.B. "Sehr geehrte/r Herr/Frau [Name],")
- Schreibe in einem professionellen Geschäftston, der nicht KI-generiert klingt
- Ende mit der Signatur "Mit freundlichen Grüßen,\\n\\nStephan Förtsch\\nExposeProfi\\ninfo@exposeprofi.de"
- KEINE EINLEITENDEN BEMERKUNGEN ODER META-KOMMENTARE - schreibe einfach die E-Mail selbst
- Halte die gesamte E-Mail prägnant (maximal 250-300 Wörter)

Richtlinien für den E-Mail-Inhalt:
1. Die Betreffzeile muss einzigartig, aufmerksamkeitserregend und speziell auf den Geschäftstyp und potenzielle Visualisierungsbedürfnisse zugeschnitten sein
2. Sprich den Empfänger mit Namen an und würdige seinen beruflichen Status
3. Beziehe dich auf den spezifischen Geschäftstyp aus dem Feld "type"
4. Wenn sie gute Bewertungen haben, erwähne kurz ihren positiven Ruf
5. Erkläre klar, wie unsere 3D-Visualisierungsdienste diesem spezifischen Geschäftstyp zugutekommen
6. Füge ein kurzes relevantes Beispiel oder eine Fallstudie ein
7. Schließe mit einer klaren, aber nicht aufdringlichen Handlungsaufforderung
8. Personalisiere basierend auf nützlichen Informationen in ihrem Profil

WICHTIG: Verwende KEINE Phrasen wie "Hier ist die E-Mail" oder "Hier ist eine personalisierte E-Mail" - beginne direkt mit "Betreff:"
"""
    
    # Prepare the request payload
    payload = {
        "stop": None,
        "model": "llama3-70b-8192",
        "top_p": 1,
        "stream": False,
        "messages": [
            {
                "role": "system",
                "content": "Du bist ein professioneller E-Mail-Verfasser, spezialisiert auf Geschäftsentwicklung. Du schreibst direkte, überzeugende E-Mails mit kreativen, personalisierten Betreffzeilen auf Deutsch. Deine E-Mails enthalten niemals Meta-Kommentare oder Erklärungen. Wenn du eine E-Mail schreibst, beginnst du direkt mit der Betreffzeile und nichts anderem davor."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": 1024,
        "temperature": 0.7  # Slightly increased temperature for more creative subject lines
    }
    
    # Estimate token usage (prompt + response)
    estimated_prompt_tokens = len(prompt.split()) * 1.3  # rough estimation
    estimated_tokens = estimated_prompt_tokens + 1024  # max output tokens
    
    # Check rate limit and wait if necessary
    wait_time = rate_limiter.request_permission(estimated_tokens)
    if wait_time > 0:
        print(f"Rate limit approaching: Waiting {wait_time} seconds before next request...")
        time.sleep(wait_time)
    
    # Make the API request
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload)
            
            # Handle rate limit response specifically
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    wait_time = 60  # Default to 60 seconds if no header
                    if 'Retry-After' in response.headers:
                        wait_time = int(response.headers['Retry-After'])
                    print(f"Rate limit exceeded: Waiting {wait_time} seconds (Attempt {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
            
            response.raise_for_status()  # Raise an exception for other HTTP errors
            
            # Parse the response
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                email_content = result['choices'][0]['message']['content']
                
                # Clean up the response if it still has introductory text
                if email_content.lower().startswith(("hier ist", "hier ist die", "das ist", "ich habe")):
                    # Find where the actual email starts (usually with "Betreff:")
                    subject_index = email_content.lower().find("betreff:")
                    if subject_index != -1:
                        email_content = email_content[subject_index:]
                
                # Record actual token usage
                usage = result.get('usage', {})
                total_tokens = usage.get('total_tokens', estimated_tokens)
                rate_limiter.record_usage(total_tokens)
                
                print(f"Request used {total_tokens} tokens")
                return email_content
            else:
                return f"Error: Unable to generate email for {lead.get('name', '')}"
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 5 * (attempt + 1)  # Exponential backoff
                print(f"API Error: {str(e)}. Retrying in {wait_time} seconds... (Attempt {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                return f"API Error after {max_retries} attempts: {str(e)}"
        except Exception as e:
            return f"Error: {str(e)}"

def parse_email_content(email_content):
    """Extract subject and body from the email content"""
    # Default values in case parsing fails
    subject = ""
    body = email_content
    
    # Find the subject line (now in German)
    if "Betreff:" in email_content:
        parts = email_content.split("Betreff:", 1)
        if len(parts) > 1:
            # Get everything after "Betreff:"
            content = parts[1].strip()
            
            # Find the first line break after the subject
            line_break_index = content.find('\n')
            if line_break_index != -1:
                subject = content[:line_break_index].strip()
                body = content[line_break_index:].strip()
            else:
                subject = content
                body = ""
    
    return subject, body

def save_email(lead, email_content, output_dir):
    """Save the generated email to a file"""
    # Create a safe filename from the business name
    safe_name = lead.get('name', 'unknown').replace(' ', '_').replace('/', '_').replace('\\', '_')
    safe_name = ''.join(c for c in safe_name if c.isalnum() or c in ['_', '-'])
    
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Write the email to a file
    file_path = os.path.join(output_dir, f"{safe_name}_email.txt")
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(email_content)
    
    return file_path

def update_json_file(email_data, json_file_path):
    """Update the JSON file with new email data in real-time"""
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(json_file_path), exist_ok=True)
    
    # Load existing data if file exists
    all_emails = []
    if os.path.exists(json_file_path):
        try:
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                all_emails = json.load(json_file)
        except json.JSONDecodeError:
            # If the file exists but is empty or invalid JSON, start with an empty list
            all_emails = []
    
    # Append new email data
    all_emails.append(email_data)
    
    # Write updated data back to file
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(all_emails, json_file, indent=2)
    
    return len(all_emails)

def main():
    # Define file paths from environment variables
    leads_csv_path = Path(os.getenv('LEADS_CSV_PATH', 'venv/data/leads.csv'))
    output_dir = Path(os.getenv('OUTPUT_DIR', 'venv/data/emails'))
    json_output_path = Path(os.getenv('JSON_OUTPUT_PATH', 'venv/data/outreach_emails.json'))
    
    # Create rate limiter with rate from environment variable
    tokens_per_minute = int(os.getenv('TOKENS_PER_MINUTE', 6000))
    rate_limiter = TokenRateLimiter(tokens_per_minute=tokens_per_minute)
    
    # Ensure the leads CSV exists
    if not leads_csv_path.exists():
        print(f"Error: {leads_csv_path} not found!")
        return
    
    # Read leads from CSV
    print(f"Reading leads from {leads_csv_path}...")
    leads = read_leads_from_csv(leads_csv_path)
    print(f"Found {len(leads)} leads")
    
    # Process each lead
    for i, lead in enumerate(leads, 1):
        print(f"Processing lead {i}/{len(leads)}: {lead.get('name', 'Unknown')}")
        
        # Skip leads without a name or email
        if not lead.get('name'):
            print("Skipping lead without a name")
            continue
        
        # Get recipient email from the lead data
        recipient_email = None
        if lead.get('emails'):
            recipient_email = lead.get('emails')
        
        if not recipient_email:
            print(f"Skipping lead without an email address: {lead.get('name')}")
            continue
        
        # Generate the email
        print("Generating personalized email in German...")
        email_content = generate_email_with_groq(lead, rate_limiter)
        
        # Save the email and add to JSON structure
        if email_content and not email_content.startswith("Error:") and not email_content.startswith("API Error:"):
            # Save to text file
            file_path = save_email(lead, email_content, output_dir)
            print(f"Email saved to {file_path}")
            
            # Parse subject and body
            subject, body = parse_email_content(email_content)
            
            # Create email data structure
            email_data = {
                "subject": subject,
                "from": os.getenv('EMAIL_FROM', 'MS_GmoyJz@trial-o65qngken68gwr12.mlsender.net'),
                "to": recipient_email,
                "body": body
            }
            
            # Update JSON file in real-time
            email_count = update_json_file(email_data, json_output_path)
            print(f"Added email to JSON file (total: {email_count}) with subject: {subject}")
        else:
            print(f"Failed to generate email: {email_content}")
        
        print("-" * 50)
    
    print(f"All emails saved to JSON file: {json_output_path}")
    print("Email generation complete!")

if __name__ == "__main__":
    main()