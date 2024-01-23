import requests
from django.shortcuts import render
import geocoder
from datetime import datetime
from django.http import JsonResponse

# Create your views here.
def temp_here(request):
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

