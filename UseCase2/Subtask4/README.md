# Task 2.4 Cost Analysis


1. First pipeline consumption:

   ![](./screenshots/pipeline-1.png)

2. Second pipeline consumption:

   ![](./screenshots/pipeline-2.png)

3. Total cost (with assumption that pipelines executes *hourly*):

<table>
<tbody>
      <tr>
         <td>
            Activity Type
         </td>
         <td>
            Runtime
         </td>
         <td>
            Count
         </td>  
         <td>
            Pipeline
         </td>  
         <td>
            Base cost per unit
         </td>  
         <td>
            Single Execution Cost
         </td>  
         <td>
            Monthly Cost
         </td>     
      </tr>
      <tr>
         <td>
            Pipeline Orchestration (Activity Runs)
         </td>
         <td>
            Azure
         </td>
         <td>
            1
         </td>  
         <td>
            CopyImdbData
         </td>  
         <td>
            0.001
         </td>  
         <td>
            0.001
         </td>  
         <td>
            0.73 (0.001 * 730)
         </td>     
      </tr>
      <tr>
         <td>
            Data Movement Activities (DIU-hour)
         </td>
         <td>
            Azure
         </td>
         <td>
            0.2
         </td>  
         <td>
            CopyImdbData
         </td>  
         <td>
            0.25
         </td>  
         <td>
            0.05 (0.2 * 0.25)
         </td>  
         <td>
            36.5 (0.05 * 730)
         </td>     
      </tr>  
      <tr>
         <td colspan="5">
         Total
         </td>
         <td>0.051</td>
         <td>37.23</td>
      </tr>
      <tr>
         <td>
            Pipeline Orchestration (Activity Runs)
         </td>
         <td>
            Azure
         </td>
         <td>
            3
         </td>  
         <td>
            IMDBBronzeToSilver
         </td>  
         <td>
            0.001
         </td>  
         <td>
            0.003
         </td>  
         <td>
            2.19 (0.003 * 730)
         </td>     
      </tr>
      <tr>
         <td>
            Data Flow (vCore-hour)
         </td>
         <td>
            Azure
         </td>
         <td>
            1.8568
         </td>  
         <td>
            IMDBBronzeToSilver
         </td>  
         <td>
            0.257
         </td>  
         <td>
            0.477
         </td>  
         <td>
            348.21 (0.477 * 730)
         </td>     
      </tr>
      <tr>
         <td colspan="5">
         Total
         </td>
         <td>0.48</td>
         <td>350.4</td>
      </tr>
</tbody>
</table>