Nentry = 1; 
fp = fopen("file_325_all.list","r");

fp_flux=fopen("Flux.tab","w");

for i=1:1:Nentry
	str = fscanf(fp,"%s",[1,1]);
	fp_data = fopen(str,"r");
	while(fgets(fp_data,1) == "#")
		fgets(fp_data);
	endwhile
	count = 0;
	current = ftell(fp_data);
	while(feof(fp_data) == 0)
		fgets(fp_data);
		count++;
	endwhile
	#count = count - 1 
	fclose(fp_data);
	fp_data = fopen(str,"r");
	while(fgets(fp_data,1) == "#")
                fgets(fp_data);
        endwhile
	
        rawdata=zeros(count,1);
	for cnt=1:1:count
		channel(cnt,1)=fscanf(fp_data,"%f",[1,1]);
                rawdata(cnt,1)=fscanf(fp_data,"%f",[1,1]);
	endfor	
	
	str

	auto = input("Enter manual (0) or auto (1) mode = ");
	if (auto == 0)
	Tsys_in = input("Enter Tsys = ");
	Ta = input("Enter Ta = ");
	Tsys = Tsys_in + Ta;
	Nant = input("Enter No of antennae = ");
        else
	Tsys_in = 66;
	Ta = 43;
	Tsys = Tsys_in + Ta;
	Nant = 23;
	#Nant = 15;
	endif

	dist = input("offset from centre in deg = ");

	plot(rawdata);
	start_offbin = input("Enter start off-pulse bin = "); 
	stop_offbin = input("Enter stop off-pulse bin = "); 
	#start_onbin1 = input("Enter start on-pulse bin-1 = "); 
        #stop_onbin1 = input("Enter stop on-pulse bin-1 = "); 
	#start_onbin2 = input("Enter start on-pulse bin-2 = "); 
        #stop_onbin2= input("Enter stop on-pulse bin-2 = ");
	obs_time =  input("Enter observation dutation in sec = ");
        norm_defl  = (max(rawdata) - mean(rawdata(start_offbin:stop_offbin)))/(mean(rawdata(start_offbin:stop_offbin)));
        defl  = (max(rawdata) - mean(rawdata(start_offbin:stop_offbin)));        
	rms = std(rawdata(start_offbin:stop_offbin));

	beamfactor = exp(-1.0*dist*dist/2/0.6/0.6);
	defl = defl/beamfactor;
	norm_defl = norm_defl/beamfactor;	

	i = 1;
	for cnt=1:1:count
		if(rawdata(cnt,1) > (mean(rawdata(start_offbin:stop_offbin)) + 3*rms))
			bin(i) = cnt;
			i = i+1;		
		endif
	endfor
	bin
        i = i-1; j=1;
	start_onbin1 = bin(1)
	while((bin(j+1) - bin(j)) == 1)	
		j = j+1;
		if(j == i) break; endif 
	endwhile
        stop_onbin1 = bin(j)
	if(j < i)
		j = j+1;	
		start_onbin2 = bin(j)
		if(j < i)	
        		while((bin(j+1) - bin(j)) == 1) 
                		j = j+1;
				if(j == i) break; endif
        		endwhile
		endif	
        	stop_onbin2 = bin(j)
	else
		start_onbin2 = stop_onbin2 = 0
	endif

	edit = input("Edit (1) or not (0) the on_bin values = ");
        if (edit == 1)
		start_onbin1 = input("Enter start on-pulse bin1 = ");
        	stop_onbin1 = input("Enter stop on-pulse bin1 = ");
		start_onbin2 = input("Enter start on-pulse bin2 = ");
        	stop_onbin2= input("Enter stop on-pulse bin2 = ");
	endif


        Peak_flux = (norm_defl*Tsys)/(0.33*sqrt(2*Nant));
	amp1=zeros(count,1);
        amp1 = rawdata - mean(rawdata(start_offbin:stop_offbin));	
	sum_amp1=0;
	sum_amp2=0;
        sum_amp1 = sum(amp1(start_onbin1:stop_onbin1));
	if(start_onbin2 == stop_onbin2)
	sum_amp2 = 0;
	else
        sum_amp2 = sum(amp1(start_onbin2:stop_onbin2));
	endif
	sum_amp1 = sum_amp1 + sum_amp2;
	TOTAL_FLUX=0; MEAN_FLUX=0;
        TOTAL_FLUX = (sum_amp1*Peak_flux)/defl;
        #TOTAL_FLUX = (sum_amp1*Peak_flux)/norm_defl;
	MEAN_FLUX = TOTAL_FLUX/count;
	MDF = (defl/rms) * (Tsys / (0.33*sqrt(2*Nant*32000000*obs_time))) * sqrt((stop_onbin1-start_onbin1+stop_onbin2-start_onbin2)/(count-(stop_onbin1-start_onbin1+stop_onbin2-start_onbin2)) );	
        fprintf(fp_flux,"%s %f %f %f %f %f %f \n", str, defl, rms, Peak_flux, TOTAL_FLUX, MEAN_FLUX, MDF);
	fclose(fp_data);
        rawdata=zeros(count,1);
        Tsys=Tsys_in;
endfor
fclose(fp);
fclose(fp_flux);
