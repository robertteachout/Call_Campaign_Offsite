import matplotlib.pyplot as plt
import numpy as np
from FileLoad import Final_Load
from Priority_Tracking import Project_Rank, Load_PR

# df = Final_Load()
df1= Load_PR('Offsite')

# df1.hist(bins=50, figsize=(15,15))
df1.plot(y='Forecasted Netcharts', x ='Pacing %' , kind='scatter')
plt.show()
