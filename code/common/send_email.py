import os
from jinja2 import Environment, FileSystemLoader
from exchangelib import DELEGATE, Account, Credentials, Configuration, Message, Mailbox, IMPERSONATION, HTMLBody, FileAttachment, Body

# env = Environment(loader=FileSystemLoader('assets'))
# template = env.get_template('email.html')
# html_content = template.render(template_vars)

def send_email(receiver_email:str,
               sender_email:str,
               sender_password:str,
               subject:str,
               email_body:str,
               is_html:bool,
               servername = 'mail.tfcu.coop',
               primary_smtp_address = 'business_intelligence@raiz.us',
               ):
    
    credentials = Credentials(username = sender_email, password = sender_password)
    

    config = Configuration(
        server = servername,
        credentials = credentials,
        )

    account = Account(
        primary_smtp_address = primary_smtp_address,
        credentials = credentials,
        config = config,
        autodiscover = True,
        access_type = DELEGATE 
        )
    
    message = Message(
        account = account,
        subject = subject,
        body = HTMLBody(email_body) if is_html else email_body,
        to_recipients = [ Mailbox(email_address = receiver_email)] 
        )
    
    message.send()

