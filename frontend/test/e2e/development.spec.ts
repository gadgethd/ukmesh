import {expect,test} from '@playwright/test';
import {fixtures,vectorRoutes} from './basemapFixtures.js';
test('development renderer and worker load through queried TypeScript modules',async({page})=>{
 const errors:string[]=[];
 page.on('pageerror',e=>errors.push(e.message));
 page.on('console',m=>{if(m.type()==='error'&&/SyntaxError|TypeError|ReferenceError/.test(m.text()))errors.push(m.text());});
 await fixtures(page);await vectorRoutes(page,()=>false);
 await page.goto('/');
 await expect(page.locator('.map-area')).toHaveAttribute('data-basemap-state','healthy',{timeout:60000});
 expect(errors).toEqual([]);
});
