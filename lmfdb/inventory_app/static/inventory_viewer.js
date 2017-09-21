
//---------- Viewer DOM creation ---------------------------------

function populateViewerPage(blockList, startVisible=true){
  //Create the HTML elements using the blocklist
  //We create in two chunks

  var specialsDiv = document.getElementById('specialsDiv');
  var dataDiv = document.getElementById('dataDiv');
  var specialFields = [];
  var keys = Object.keys(blockList.blockList).sort();
  console.log(keys);
  var uniq_keys = {};
  for(var i=0; i<keys.length; i++){
    var str = keys[i];
    //Strip out field, leaving the 'Box_' bit
    var head = str.substr(1,str.lastIndexOf('_')-1 );
    uniq_keys[head] = 0;
  }
  fields = Object.keys(uniq_keys);

  var table_div = document.createElement('div');
  var table = document.createElement('table');
  table.class = 'viewerTable';
  entry_styles = ['table_tag'];

  var row = createViewerRow(blockList, 'Key', '', header=true)
  table.appendChild(row);
  for(var i=0; i < fields.length; i++){
    row = createViewerRow(blockList, fields[i].substr(4, fields[i].length), '#'+fields[i]+'_');
    if(row){
      if(i%2 == 0){
        row.classList.add('viewerTableEven');
      }else{
        row.classList.add('viewerTableOdd');
      }
      table.appendChild(row);
    }else{
      specialFields.push(i);
    }
  }

  table_div.appendChild(table);
  dataDiv.appendChild(table_div);

  createInfoTable(blockList, fields[specialFields[0]], fields[specialFields[1]], specialsDiv);

  $( document ).trigger( "blockListReady");
}

function createViewerRow(blockList, field, id_start, header=false){

  var table_row = document.createElement('tr');
  var table_el = document.createElement('td');
  var clas = header ? 'viewerTableHeaders' : 'viewerTableEls';
  var special = false;
  table_el.innerHTML = field;
  table_el.classList.add(clas);
  table_row.appendChild(table_el);

  for(var j=0; j < table_fields.length; j++){
    var table_el = document.createElement('td');
    table_el.classList.add(clas);
    if(header){
      table_el.innerHTML = capitalise(table_fields[j]);
    }else{
      var block = blockList.getBlock(id_start+table_fields[j]);
      if(block && ! block.special){
        table_el.innerHTML = block.text;
      }else{
        special = true;
      }
    }
    table_row.appendChild(table_el);
  }
  return (special ? null : table_row);

}

function createInfoTable(blockList, info, notes){

  var table = document.createElement('table');
  for(var j=0; j< info_fields.length; j++){
    var table_row = document.createElement('tr');
    var el1 = document.createElement('td');
    el1.classList.add('viewerTableHeaders');
    el1.innerHTML = capitalise(info_fields[j]);
    table_row.appendChild(el1);
    var el2 = document.createElement('td');
    el2.classList.add('viewerTableEls');
    var block = blockList.getBlock('#'+info+'_'+info_fields[j]);
    if(block) el2.innerHTML = block.text;
    if(j%2 == 0){
      table_row.classList.add('viewerTableEven');
    }else{
      table_row.classList.add('viewerTableOdd');        
    }

    table_row.appendChild(el2);
    table.appendChild(table_row);
  }

  specialsDiv.appendChild(table);

  var div = document.createElement('div');
  div.innerHTML = "NOTES: " +blockList.getBlock('#Box_NOTES_description').text;
  console.log(div.innerHTML);
  specialsDiv.appendChild(div);

}

//---------- General block and list handling ---------------------
