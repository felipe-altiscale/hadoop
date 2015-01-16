/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function() {
  "use strict";

  // The chunk size of tailing the files, i.e., how many bytes will be shown
  // in the preview.
  var TAIL_CHUNK_SIZE = 32768;

  var current_directory = "";

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  $(window).bind('hashchange', function () {
    $('#alert-panel').hide();

    var dir = window.location.hash.slice(1);
    if(dir == "") {
      dir = "/";
    }
    if(current_directory != dir) {
      browse_directory(dir);
    }
  });

  function network_error_handler(url) {
    return function (jqxhr, text, err) {
      switch(jqxhr.status) {
        case 401:
          var msg = '<p>Authentication failed when trying to open ' + url + ': Unauthorized.</p>';
          break;
        case 403:
          if(jqxhr.responseJSON !== undefined && jqxhr.responseJSON.RemoteException !== undefined) {
            var msg = '<p>' + jqxhr.responseJSON.RemoteException.message + "</p>";
            break;
          }
          var msg = '<p>Permission denied when trying to open ' + url + ': ' + err + '</p>';
          break;
        case 404:
          var msg = '<p>Path does not exist on HDFS or WebHDFS is disabled.  Please check your path or enable WebHDFS</p>';
          break;
        default:
          var msg = '<p>Failed to retrieve data from ' + url + ': ' + err + '</p>';
        }
      show_err_msg(msg);
    };
  }

  function append_path(prefix, s) {
    var l = prefix.length;
    var p = l > 0 && prefix[l - 1] == '/' ? prefix.substring(0, l - 1) : prefix;
    return p + '/' + s;
  }

  function get_response(data, type) {
    return data[type] !== undefined ? data[type] : null;
  }

  function get_response_err_msg(data) {
    return data.RemoteException !== undefined ? data.RemoteException.message : "";
  }

  /* This method loads the checkboxes on the permission info modal. It accepts
   * the string representation, eg. '-rwxr-xr-t' or 'drwxrwx---' and infers the
   * checkboxes that should be true and false
   */
  function view_perm_details(path, abs_path, perms) {
    $('#perm-info-title').text("Permissions information - " + path);

    var arr = ["#sticky", "#perm-ur", "#perm-uw", "#perm-ux", "#perm-gr",
               "#perm-gw", "#perm-gx", "#perm-or", "#perm-ow", "#perm-ox"]

    //Sticky bit could be set on first char (dir) or last (file)
    $( arr[0] ).prop( 'checked',
      perms.charAt(0) == 't' || perms.charAt(9) == 't' );

    for(var i = 1; i < perms.length; i++) {
      $( arr[i] ).prop( 'checked', perms.charAt(i) != '-');
    }

    $('#perm-info').modal();
  }

  /* Figure out the Octal permissions from the checkboxes
   */
  function convertCheckboxesToOctalPermissions() {
    var octal = "";

    octal += $( '#sticky' ).prop( 'checked' ) ? "1" : "0";

  }

  // Use WebHDFS to set permissions on an absolute path
  function setPermission(abs_path, permissionMask) {
    // PUT /webhdfs/v1/<path>?op=SETPERMISSION&permission=<permission>
    var url = '/webhdfs/v1' + abs_path + '?op=SETPERMISSION'
        + '&permission=' + permissionMask;

    $.ajax(url,
      { type: 'PUT',
        crossDomain: true
      }).done(function(data) {
      var d = get_response(data, "FileStatuses");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }
    }).error(network_error_handler(url));
  }

  
  function view_file_details(path, abs_path) {
    function show_block_info(blocks) {
      var menus = $('#file-info-blockinfo-list');
      menus.empty();

      menus.data("blocks", blocks);
      menus.change(function() {
        var d = $(this).data('blocks')[$(this).val()];
        if (d === undefined) {
          return;
        }

        dust.render('block-info', d, function(err, out) {
          $('#file-info-blockinfo-body').html(out);
        });

      });
      for (var i = 0; i < blocks.length; ++i) {
        var item = $('<option value="' + i + '">Block ' + i + '</option>');
        menus.append(item);
      }
      menus.change();
    }

    var url = '/webhdfs/v1' + abs_path + '?op=GET_BLOCK_LOCATIONS';
    $.get(url).done(function(data) {
      var d = get_response(data, "LocatedBlocks");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      $('#file-info-tail').hide();
      $('#file-info-title').text("File information - " + path);

      var download_url = '/webhdfs/v1' + abs_path + '?op=OPEN';

      $('#file-info-download').attr('href', download_url);
      $('#file-info-preview').click(function() {
        var offset = d.fileLength - TAIL_CHUNK_SIZE;
        var url = offset > 0 ? download_url + '&offset=' + offset : download_url;
        $.get(url, function(t) {
          $('#file-info-preview-body').val(t);
          $('#file-info-tail').show();
        }, "text").error(network_error_handler(url));
      });

      if (d.fileLength > 0) {
        show_block_info(d.locatedBlocks);
        $('#file-info-blockinfo-panel').show();
      } else {
        $('#file-info-blockinfo-panel').hide();
      }
      $('#file-info').modal();
    }).error(network_error_handler(url));
  }

  function browse_directory(dir) {
    var url = '/webhdfs/v1' + dir + '?op=LISTSTATUS';
    $.get(url, function(data) {
      var d = get_response(data, "FileStatuses");
      if (d === null) {
        show_err_msg(get_response_err_msg(data));
        return;
      }

      current_directory = dir;
      $('#directory').val(dir);
      window.location.hash = dir;
      dust.render('explorer', d, function(err, out) {
        $('#panel').html(out);

        $('.explorer-browse-links').click(function() {
          var type = $(this).attr('inode-type');
          var path = $(this).attr('inode-path');
          var abs_path = append_path(current_directory, path);
          if (type == 'DIRECTORY') {
            browse_directory(abs_path);
          } else {
            view_file_details(path, abs_path);
          }
        });

        $('.explorer-perm-links').click(function() {
          var path = $(this).attr('inode-path');
          var perms = $(this).html();
          var abs_path = append_path(current_directory, path);
          view_perm_details(path, abs_path, perms);
        });

      });
    }).error(network_error_handler(url));
  }


  function init() {
    dust.loadSource(dust.compile($('#tmpl-explorer').html(), 'explorer'));
    dust.loadSource(dust.compile($('#tmpl-block-info').html(), 'block-info'));

    var b = function() { browse_directory($('#directory').val()); };
    $('#btn-nav-directory').click(b);
    var dir = window.location.hash.slice(1);
    if(dir == "") {
      window.location.hash = "/";
    } else {
      browse_directory(dir);
    }
  }

  $('#upload-file').on('show.bs.modal', function(event) {
    var pwd = window.location.hash.slice(1);
    if( pwd.slice(-1) != '/' ) {
      pwd = pwd + '/';
    }

    var modal = $(this)
    $('#upload-file-button').on('click', function() {
      $(this).prop('disabled', true);
      $(this).button('complete');

      for(var i = 0; i < $('#upload-file-input').prop('files').length; i++) {
        var file = $('#upload-file-input').prop('files')[i];
        var url = '/webhdfs/v1' + pwd + file.name + '?op=CREATE';

        $.ajax({
          type: 'PUT',
          url: url,
          data: file,
          processData: false,
          crossDomain: true
        }).done(function(data) {
          modal.modal('hide');
          browse_directory(pwd);
        });
      }
    });
  });


  init();
})();
