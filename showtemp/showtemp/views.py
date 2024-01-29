import requests
from django.shortcuts import render
import geocoder
from datetime import datetime
from django.http import JsonResponse
import json
from django.views.decorators.csrf import csrf_exempt

# Create your views here.
@csrf_exempt
def temp_here(request):
    if request.method == 'GET':
        location = geocoder.ip('me').latlng
        endpoint = "https://api.open-meteo.com/v1/forecast"
        api_request = f"{endpoint}?latitude={location[0]}&longitude={location[1]}&hourly=temperature_2m"
        res = requests.get(api_request).json()
        print(str(res))
        now = datetime.now()
        hour = now.hour
        meteo_data = res
        temp = meteo_data['hourly']['temperature_2m'][hour]
        # return HttpResponse(f"Here it's {temp} C") 

        
        # context = {'temp': temp, 'res': res}

        # For a regular HTML response
        # return render(request, 'your_template.html', context)

        # For a JsonResponse (for AJAX or API response)
        # return JsonResponse(context)
        return JsonResponse(res)
    elif request.method == 'POST':
        return JsonResponse({'msg':'hey, please make a get request for this route.'})

@csrf_exempt
def handle_post_request(request):
    if request.method == 'POST':
        try:
            # Access the raw request body
            raw_body = request.body

            # Convert the raw body to a string (assuming it's in JSON format)
            body_str = raw_body.decode('utf-8')

            # Parse the JSON data
            json_data = json.loads(body_str)

            # Your processing/db operations with json_data
            
            return JsonResponse({'message': 'Data received. You sent this data to us', 'data': json_data})
        except json.JSONDecodeError as e:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    
    else:
        # If it's not a POST request, you can handle it accordingly
        return JsonResponse({'msg': 'Hey, please make a POST request for this route.'})