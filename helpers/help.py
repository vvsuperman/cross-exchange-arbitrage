import requests
import logging

WEBHOOK_URL = "https://oapi.dingtalk.com/robot/send?access_token=7b588bb6ca84b8b7e8ff97af90021a12ac2c56e96e75670201318ab313779459"


def send_webhook_alert(msg: str):
        """Send webhook alert for new token discovery"""
        
        message ={"msgtype": "text", 
         "text": {"content": f"exchange监控消息提醒,lighter异常: {msg}"} }
        
        headers = {
            'Content-Type': 'application/json'
        }

        try:
            response = requests.post(WEBHOOK_URL, json=message, headers=headers, timeout=10)

            if response.status_code == 200:
                logging.info(f"Webhook alert sent for ({msg})")
            else:
                logging.info(f"Failed to send webhook: {msg}")
        except Exception as e:
            logging.info(f"Error sending webhook: {e}")